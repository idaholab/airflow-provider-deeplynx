from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from airflow.configuration import conf
from deeplynx_provider.hooks.deeplynx import DeepLynxHook
from deeplynx_provider.operators.utils import reconstruct_config_str
from deep_lynx.configuration import Configuration
import deep_lynx
import json
import os

class DeepLynxBaseOperator(BaseOperator):
    """
    DeepLynxBaseOperator is the base for the majority of the operators in deeplynx_provider.
    Used to perform common setup and configuration for interacting with the DeepLynx API. Subclasses must
    implement the `do_custom_logic` method to define the specific logic for the operator.

    Attributes:
        token (str, optional): The token for authentication.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
    """

    template_fields = ('token', 'conn_id', 'host', 'deeplynx_config')

    @apply_defaults
    def __init__(self, token: str = None, conn_id: str = None, host: str = None, deeplynx_config: dict = None, *args, **kwargs):
        """
        Initialize DeepLynxBaseOperator with the given parameters.

        Args:
            token (str, optional): The token for authentication.
            conn_id (str, optional): The connection ID to use.
            host (str, optional): The host for the DeepLynx API.
            deeplynx_config (dict, optional): Additional configuration for DeepLynx.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)
        self.token = token
        self.conn_id = conn_id
        self.host = host
        self.deeplynx_config = deeplynx_config

        self.validate_params()

    def execute(self, context):
        """
        Execute the operator's main logic.

        This method sets up the DeepLynx configuration, creates a DeepLynxHook,
        and calls the `do_custom_logic` method which must be implemented by subclasses.

        Args:
            context (dict): The context dictionary provided by Airflow.

        Returns:
            The result of the `do_custom_logic` method.
        """
        self.config = self.get_deeplynx_config()
        deeplynx_hook = DeepLynxHook(self.config, self.token)
        return self.do_custom_logic(context, deeplynx_hook)

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Define the custom logic for the operator.

        This method must be implemented by subclasses to perform specific tasks.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        raise NotImplementedError("Subclasses must implement this method.")

    def validate_params(self):
        """
        Validate the parameters provided to the operator.

        Ensure that only one of `conn_id`, `host`, or `deeplynx_config` is provided,
        and that at least one of them is present. Also ensure that a token is provided.

        Raises:
            AirflowException: If the validation fails.
        """
        if sum([self.conn_id is not None, self.host is not None, self.deeplynx_config is not None]) >= 2:
            raise AirflowException("Please provide only one of conn_id, host, or deeplynx_config.")
        elif self.conn_id is None and self.host is None and self.deeplynx_config is None:
            raise AirflowException("Please provide either a conn_id, a host, or deeplynx_config.")
        elif self.token is None:
            raise AirflowException("Please provide a token.")

    def get_deeplynx_config(self):
        """
        Retrieve the DeepLynx configuration.

        This method constructs the DeepLynx configuration using either `deeplynx_config`,
        `conn_id`, or `host`, and ensures required SSL and temp folder settings are present.

        Returns:
            Configuration: The DeepLynx configuration object.
        """

        # prefer to use deeplynx_config if provided
        if self.deeplynx_config is not None:
            config = reconstruct_config_str(self.deeplynx_config)
        elif self.conn_id is not None:
            conn = BaseHook.get_connection(self.conn_id)
            config = Configuration()
            config.host = conn.host
        elif self.host is not None:
            config = Configuration()
            config.host = self.host

        ## ensure config has required ssl and temp_folder_path info
        # add ssl_ca_cert if its not already provided by self.deeplynx_config
        if config.ssl_ca_cert is None:
            env_cert = os.getenv('SSL_CERT_FILE', None)
            if env_cert is not None:
                config.ssl_ca_cert = env_cert
            else:
                config.verify_ssl = False

        ## add temp_folder
        if config.temp_folder_path is None:
            env_temp_path = os.getenv('DEEPLYNX_DATA_TEMP_FOLDER', None)
            if env_temp_path is not None:
                config.temp_folder_path = env_temp_path
            else:
                log_folder = conf.get('logging', 'base_log_folder')
                data_folder = f"{log_folder}/data"
                config.temp_folder_path = data_folder

        if not os.path.exists(config.temp_folder_path):
            os.makedirs(config.temp_folder_path)

        return config

    def write_or_push_to_xcom(self, context, data, file_name):
        """
        Write data to a file or push it to XCom.

        Args:
            context (dict): The context dictionary provided by Airflow.
            data (str): The data to write or push.
            file_name (str): The name of the file to write the data to.

        Returns:
            None
        """
        task_instance = context['task_instance']
        if self.write_to_file is not None:
            file_path = self.save_data(data, file_name)
            task_instance.xcom_push(key='file_path', value=file_path)
        else:
            task_instance.xcom_push(key='data', value=data)

    def save_data(self, data, file_name):
        """
        Save data to a local file.

        Args:
            data (str): The data to save.
            file_name (str): The name of the file to save the data to.

        Returns:
            str: The path of the saved file.
        """
        if self.config.temp_folder_path:
            return self.write_data_to_local(file_name, data)
        else:
            raise ValueError("temp_folder must be provided")

    def write_data_to_local(self, file_name, data):
        """
        Write data to a local file.

        Args:
            file_name (str): The name of the file.
            data (str): The data to write.

        Returns:
            str: The path of the saved file.
        """
        file_path = os.path.join(self.config.temp_folder_path, file_name)
        self.write_data(file_path, data)
        return file_path

    def write_data(self, file_path, data):
        """
        Write data to a specified file path.

        Args:
            file_path (str): The path of the file.
            data (str): The data to write.

        Returns:
            None
        """
        with open(file_path, 'w') as f:
            f.write(data)

    def format_query_response_filename(self, context, query_name):
        """
        Format the filename for query response data.

        Args:
            context (dict): The context dictionary provided by Airflow.
            query_name (str): The name of the query.

        Returns:
            str: The formatted filename.
        """
        run_id = context['run_id']
        task_id = context['task'].task_id
        query_response_filename = query_name + '_' + run_id + '_' + task_id + '.json'
        return query_response_filename
