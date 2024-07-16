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
    template_fields = ('token', 'conn_id', 'host', 'deeplynx_config')

    @apply_defaults
    def __init__(self, token:str=None, conn_id:str=None, host:str=None, deeplynx_config:dict=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.token = token
        self.conn_id = conn_id
        self.host = host
        self.deeplynx_config = deeplynx_config
        #
        self.validate_params()

    def execute(self, context):
        ## deep_lynx.configuration
        self.config = self.get_deeplynx_config()
        ## get deeplynx_hook
        deeplynx_hook = DeepLynxHook(self.config, self.token)
        ## call method that must be implemented by each subclass
        return self.do_custom_logic(context, deeplynx_hook)

    def do_custom_logic(self, context, deeplynx_hook):
        raise NotImplementedError("Subclasses must implement this method.")

    def validate_params(self):
        # only one of conn_id, or host, or deeplynx_config
        if sum([self.conn_id is not None, self.host is not None, self.deeplynx_config is not None]) >= 2:
            raise AirflowException("Please provide only one of conn_id, host, or deeplynx_config.")
        # at least one of conn_id, or host, or deeplynx_config
        elif self.conn_id is None and self.host is None and self.deeplynx_config is None:
            raise AirflowException("Please provide either a conn_id, a host, or deeplynx_config.")
        elif self.token is None:
            raise AirflowException("Please provide a token.")

    #############################################################################
    ##### deeplynx config
    # logic for all DeepLynxBaseOperator derived operators to get their deep_lynx sdk config
    def get_deeplynx_config(self):
        #### get config from either deeplynx_config, conn_id or host
        # prefer to use deeplynx_config if provided
        if self.deeplynx_config is not None:
            # Use provided deeplynx_config
            config = reconstruct_config_str(self.deeplynx_config)
        # elif conn_id is provided, use it to construct deeplynx config
        elif self.conn_id is not None:
            conn = BaseHook.get_connection(self.conn_id)
            config = Configuration()
            config.host = conn.host
        # elif host is provided, use it to construct deeplynx config
        elif self.host is not None:
            config = Configuration()
            config.host = self.host

        #### ensure config has required ssl and temp_folder_path info
        ## add ssl_ca_cert if its not already provided by self.deeplynx_config
        if config.ssl_ca_cert is None:
            env_cert = os.getenv('SSL_CERT_FILE', None)
            if env_cert is not None:
                config.ssl_ca_cert = env_cert
            # if no cert provided, default to verify_ssl = False
            else:
                config.verify_ssl = False
        ## add temp_folder if it wasn't already provided by self.deeplynx_config
        if config.temp_folder_path is None:
            env_temp_path = os.getenv('DEEPLYNX_DATA_TEMP_FOLDER', None)
            # add temp folder via env var if its provided
            if env_temp_path is not None:
                config.temp_folder_path = env_temp_path
            # otherwise use the default temp folder location
            else:
                log_folder = conf.get('logging', 'base_log_folder')
                data_folder = f"{log_folder}/data"
                config.temp_folder_path = data_folder
        ## Check if config.temp_folder_path exists, if not, create it
        if not os.path.exists(config.temp_folder_path):
            os.makedirs(config.temp_folder_path)

        ####
        return config

    #############################################################################
    ##### data storage
    # many deeplynx operators retrieve json data that should either be writen to storage or the data should be passed to xcom directly
    def write_or_push_to_xcom(self, context, data, file_name):
        import os
        import json

        task_instance = context['task_instance']
        if self.write_to_file is not None:
            file_path = self.save_data(data, file_name)
            # Push file_path to XCom
            task_instance.xcom_push(key='file_path', value=file_path)
        else:
            # Push data to XCom
            task_instance.xcom_push(key='data', value=data)

    def save_data(self, data, file_name):
        if self.config.temp_folder_path:
            # Store data locally
            return self.write_data_to_local(file_name, data)
        else:
            raise ValueError("temp_folder must be provided")

    def write_data_to_local(self, file_name, data):
        file_path = os.path.join(self.config.temp_folder_path, file_name)
        self.write_data(file_path, data)
        return file_path

    def write_data(self, file_path, data):
        # Write data to the specified file path
        with open(file_path, 'w') as f:
            f.write(data)

    def format_query_response_filename(self, context, query_name):
        run_id = context['run_id']
        task_id = context['task'].task_id
        query_response_filename = query_name + '_' + run_id + '_' + task_id + '.json'

        return query_response_filename

    #############################################################################
