from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from deep_lynx.configuration import Configuration
import logging
import multiprocessing
import json
import os

class DeepLynxConfigurationOperator(BaseOperator):
    """
    Create and configure a DeepLynx configuration object.

    This operator requires either a connection ID or a host, and can be customized
    with various optional parameters for API key management, SSL verification,
    and proxy settings.

    Attributes:
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        temp_folder_path (str, optional): The temporary folder path for file storage.
        api_key (dict, optional): The API key for authentication.
        api_key_prefix (dict, optional): The API key prefix.
        refresh_api_key_hook (str, optional): The hook to refresh the API key.
        username (str, optional): The username for authentication.
        password (str, optional): The password for authentication.
        debug (bool, optional): The debug mode flag.
        verify_ssl (bool, optional): The flag to verify SSL certificates.
        ssl_ca_cert (str, optional): The path to the SSL CA certificate.
        cert_file (str, optional): The path to the client certificate file.
        key_file (str, optional): The path to the client key file.
        assert_hostname (str, optional): The hostname to verify.
        connection_pool_maxsize (int, optional): The maximum size of the connection pool.
        proxy (str, optional): The proxy settings.
        safe_chars_for_path_param (str, optional): Safe characters for path parameters.
    """

    template_fields = (
        'conn_id',
        'host',
        'temp_folder_path',
        'api_key',
        'api_key_prefix',
        'refresh_api_key_hook',
        'username',
        'password',
        'debug',
        'verify_ssl',
        'ssl_ca_cert',
        'cert_file',
        'key_file',
        'assert_hostname',
        'connection_pool_maxsize',
        'proxy',
        'safe_chars_for_path_param'
    )

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
        """
        Initialize DeepLynxConfigurationOperator with the given parameters.

        Args:
            conn_id (str, optional): The connection ID to use for the operation.
            host (str, optional): The host for the DeepLynx API.
            temp_folder_path (str, optional): The temporary folder path for file storage.
            api_key (dict, optional): The API key for authentication.
            api_key_prefix (dict, optional): The API key prefix.
            refresh_api_key_hook (str, optional): The hook to refresh the API key.
            username (str, optional): The username for authentication.
            password (str, optional): The password for authentication.
            debug (bool, optional): The debug mode flag.
            verify_ssl (bool, optional): The flag to verify SSL certificates.
            ssl_ca_cert (str, optional): The path to the SSL CA certificate.
            cert_file (str, optional): The path to the client certificate file.
            key_file (str, optional): The path to the client key file.
            assert_hostname (str, optional): The hostname to verify.
            connection_pool_maxsize (int, optional): The maximum size of the connection pool.
            proxy (str, optional): The proxy settings.
            safe_chars_for_path_param (str, optional): Safe characters for path parameters.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(*args, **kwargs)

        self.conn_id = conn_id
        self.host = host
        self.temp_folder_path = temp_folder_path
        self.api_key = api_key or {}
        self.api_key_prefix = api_key_prefix or {}
        self.refresh_api_key_hook = refresh_api_key_hook
        self.username = username
        self.password = password
        self.debug = debug
        self.ssl_ca_cert = ssl_ca_cert
        self.verify_ssl = verify_ssl
        self.cert_file = cert_file
        self.key_file = key_file
        self.assert_hostname = assert_hostname
        self.connection_pool_maxsize = connection_pool_maxsize or (multiprocessing.cpu_count() * 5)
        self.proxy = proxy
        self.safe_chars_for_path_param = safe_chars_for_path_param
        self.config = None

    def execute(self, context):
        """
        Execute the configuration creation for DeepLynx.

        This method sets up the DeepLynx configuration based on the provided
        parameters, validates the input, and logs the configuration details.

        Args:
            context (dict): The context dictionary provided by Airflow.

        Returns:
            None
        """
        # get host from conn_id if provided
        if self.conn_id is not None:
            conn = BaseHook.get_connection(self.conn_id)
            self.host = conn.host
        elif self.host is not None:
            self.host = self.host
        else:
            raise AirflowException("Must supply either conn_id or host, at a minimum.")

        # create new config and assign values
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
