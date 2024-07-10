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
        self.ssl_ca_cert = os.getenv('SSL_CERT_FILE', None)
        ## set temp folder; use env var or default
        self.set_temp_folder()
        #
        self.validate_params()

    def execute(self, context):
        ## deep_lynx.configuration
        config = self.get_deeplynx_config()

        # TODO: could add further checks for specific or type of operators; deep_lynx.download_file needs additional config for example
        deeplynx_hook = DeepLynxHook(config, self.token)

        # call method that must be implemented by each subclass
        return self.do_custom_logic(context, deeplynx_hook)

    def do_custom_logic(self, context, deeplynx_hook):
        raise NotImplementedError("Subclasses must implement this method.")

    def validate_params(self):
        ## param checks
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
        # TODO: could add more deeplynx config with conn_id
        if self.conn_id is not None:
            # If conn_id is provided, use it to construct deeplynx config
            conn = BaseHook.get_connection(self.conn_id)
            config = Configuration()
            config.host = conn.host
        elif self.deeplynx_config is not None:
            # Use provided deeplynx_config
            config = reconstruct_config_str(self.deeplynx_config)
        elif self.host is not None:
            # lastly, If host is provided, use it to construct deeplynx config
            config = Configuration()
            config.host = self.host

        ## add ssl_ca_cert if its provided
        if self.ssl_ca_cert is not None:
            config.ssl_ca_cert = self.ssl_ca_cert
        # TODO: should I default to verify_ssl = False if no cert?
        else:
            config.verify_ssl = False

        ## ensure host has https
        config.host = config.host

        ## add temp_folder
        config.temp_folder_path = self.temp_folder

        return config

    #############################################################################
    ##### data storage
    def set_temp_folder(self):
        log_folder = conf.get('logging', 'base_log_folder')
        data_folder = f"{log_folder}/data"
        self.temp_folder = os.getenv('DEEPLYNX_DATA_TEMP_FOLDER', data_folder)
        # Check if the folder exists, if not, create it
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)

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
        if self.temp_folder:
            # Store data locally
            return self.write_data_to_local(file_name, data)
        else:
            raise ValueError("temp_folder must be provided")

    def write_data_to_local(self, file_name, data):
        file_path = os.path.join(self.temp_folder, file_name)
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
