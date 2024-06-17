from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from deep_lynx.configuration import Configuration
import logging
import multiprocessing
import json
import os

class DeepLynxConfigurationOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 host=None,
                 temp_folder_path=None,
                 api_key=None,
                 api_key_prefix=None,
                 refresh_api_key_hook=None,
                 username='',
                 password='',
                 debug=False,
                 verify_ssl=True,
                 ssl_ca_cert=None,
                 cert_file=None,
                 key_file=None,
                 assert_hostname=None,
                 connection_pool_maxsize=None,
                 proxy=None,
                 safe_chars_for_path_param='',
                 *args,
                 **kwargs):
        super(DeepLynxConfigurationOperator, self).__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.host = host
        self.temp_folder_path = temp_folder_path
        self.api_key = api_key or {}
        self.api_key_prefix = api_key_prefix or {}
        self.refresh_api_key_hook = refresh_api_key_hook
        self.username = username
        self.password = password
        self.debug = debug
        self.ssl_ca_cert = ssl_ca_cert or os.getenv('SSL_CERT_FILE', None)
        self.verify_ssl = verify_ssl if self.ssl_ca_cert else False
        self.cert_file = cert_file
        self.key_file = key_file
        self.assert_hostname = assert_hostname
        self.connection_pool_maxsize = connection_pool_maxsize or (multiprocessing.cpu_count() * 5)
        self.proxy = proxy
        self.safe_chars_for_path_param = safe_chars_for_path_param
        self.config = None

        # get host from conn_id if provided
        if conn_id is not None:
            conn = BaseHook.get_connection(self.conn_id)
            self.host = conn.host
        elif host is not None:
            self.host = host
        else:
            raise AirflowException("Must supply either conn_id or host, at a minimum.")

    def execute(self, context):
        config = Configuration()
        config.host = self.host
        config.temp_folder_path = self.temp_folder_path
        config.api_key = self.api_key
        config.api_key_prefix = self.api_key_prefix
        config.refresh_api_key_hook = self.refresh_api_key_hook
        config.username = self.username
        config.password = self.password

        config.debug = self.debug
        config.verify_ssl = self.verify_ssl
        config.ssl_ca_cert = self.ssl_ca_cert
        config.cert_file = self.cert_file
        config.key_file = self.key_file
        config.assert_hostname = self.assert_hostname
        config.connection_pool_maxsize = self.connection_pool_maxsize
        config.proxy = self.proxy
        config.safe_chars_for_path_param = self.safe_chars_for_path_param

        self.config = config

        # Log configuration details
        self.log.info(f'Configuration created with host: {self.host}')

        # Filter out non-serializable objects
        config_dict = {
            k: v for k, v in self.config.__dict__.items()
            if not callable(v) and not isinstance(v, (logging.Logger, logging.Handler, logging.Formatter, type(None)))
        }
        # Manually remove nested non-serializable objects
        config_dict.pop('logger', None)
        config_dict.pop('logger_formatter', None)
        config_dict.pop('logger_stream_handler', None)

        self.log.info(f'Configuration details: {config_dict}')

        # Convert the dictionary to a JSON string and push it to XCom
        config_json = json.dumps(config_dict)
        context['ti'].xcom_push(key='deeplynx_config', value=config_json)
