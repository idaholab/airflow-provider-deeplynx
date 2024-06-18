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
    template_fields = ('token', 'conn_id', 'deeplynx_config', 'minio_uri')

    @apply_defaults
    def __init__(self, token: str, conn_id:str=None, host:str=None, deeplynx_config:dict=None, minio_uri=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        ## param checks
        # only one of conn_id, or host, or deeplynx_config
        if sum([conn_id is not None, host is not None, deeplynx_config is not None]) >= 2:
            raise AirflowException("Please provide only one of conn_id, host, or deeplynx_config.")
        elif conn_id is None and host is None and deeplynx_config is None:
            raise AirflowException("Please provide either a conn_id, a host, or deeplynx_config.")

        #
        self.token = token
        self.conn_id = conn_id
        self.host = host
        self.deeplynx_config = deeplynx_config
        self.minio_uri = minio_uri
        self.ssl_ca_cert = os.getenv('SSL_CERT_FILE', None)
        ## set temp folder; use env var or default
        self.set_temp_folder()

    def execute(self, context):
        ## deep_lynx.configuration
        config = self.get_deeplynx_config()

        # TODO: could add further checks for specific or type of operators; deep_lynx.download_file needs additional config for example
        deeplynx_hook = DeepLynxHook(config, self.token)

        # call method that must be implemented by each subclass
        return self.do_custom_logic(context, deeplynx_hook)

    def do_custom_logic(self, context, deeplynx_hook):
        raise NotImplementedError("Subclasses must implement this method.")

    def set_temp_folder(self):
        log_folder = conf.get('logging', 'base_log_folder')
        data_folder = f"{log_folder}/data"
        self.temp_folder = os.getenv('DEEPLYNX_DATA_TEMP_FOLDER', data_folder)
        # self.temp_folder = os.getenv('DEEPLYNX_DATA_TEMP_FOLDER', os.path.join(data_folder, context['dag'].dag_id, context['task'].task_id, context['run_id'])) # TODO: default temp could be more organized
        #
        # Check if the folder exists, if not, create it
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)


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
        https_host = self.ensure_https(config.host)
        config.host = https_host

        ## add temp_folder
        config.temp_folder_path = self.temp_folder

        return config

    def ensure_https(self, url):
        if not url.startswith("https://"):
            if url.startswith("http://"):
                url = url.replace("http://", "https://", 1)
            else:
                url = "https://" + url
        return url

    # many deeplynx operators retrieve json data that should either be writen to storage or the data should be passed to xcom directly
    def write_or_push_to_xcom(self, context, data, file_name):
        import os
        import json

        task_instance = context['task_instance']
        if self.minio_uri is not None or self.write_to_file is not None:
            file_path = self.save_data(data, file_name)
            # Push file_path to XCom
            task_instance.xcom_push(key='file_path', value=file_path)
        else:
            # Push data to XCom
            task_instance.xcom_push(key='data', value=data)

    #############################################################################
    def save_data(self, data, file_name):
        if self.minio_uri:
            # Store data in MinIO
            return self.write_data_and_send_to_minio(file_name, data)
        elif self.temp_folder:
            # Store data locally
            return self.write_data_to_local(file_name, data)
        else:
            raise ValueError("Either temp_folder or minio_uri must be provided")

    def write_data_to_local(self, file_name, data):
        file_path = os.path.join(self.temp_folder, file_name)
        self.write_data(file_path, data)
        return file_path

    def write_data_and_send_to_minio(self, file_name, data):
        local_path = self.write_data_to_local(file_name, data)
        self.send_data_to_minio(file_name, local_path)
        return local_path

    def send_data_to_minio(self, file_name, local_path):
        s3_hook = S3Hook(aws_conn_id='minio_default')
        bucket_name, key = self.parse_minio_uri(self.minio_uri, file_name)
        s3_hook.load_file(filename=local_path, key=key, bucket_name=bucket_name)

    def write_data(self, file_path, data):
        # Write data to the specified file path
        with open(file_path, 'w') as f:
            f.write(data)

    @staticmethod
    def parse_minio_uri(minio_uri, file_name):
        # Assuming minio_uri is in the format "s3://bucket_name/path"
        uri_parts = minio_uri.replace("s3://", "").split('/', 1)
        bucket_name = uri_parts[0]
        key = f"{uri_parts[1]}/{file_name}" if len(uri_parts) > 1 else file_name
        return bucket_name, key

    def format_query_response_filename(self, context, query_name):
        run_id = context['run_id']
        task_id = context['task'].task_id
        print("task_id")
        print(task_id)
        query_response_filename = query_name + '_' + run_id + '_' + task_id + '.json'

        return query_response_filename
