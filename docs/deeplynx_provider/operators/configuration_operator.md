Module deeplynx_provider.operators.configuration_operator
=========================================================






Classes
-------

`DeepLynxConfigurationOperator(conn_id=None, host=None, temp_folder_path=None, api_key=None, api_key_prefix=None, refresh_api_key_hook=None, username='', password='', debug=False, verify_ssl=True, ssl_ca_cert=None, cert_file=None, key_file=None, assert_hostname=None, connection_pool_maxsize=None, proxy=None, safe_chars_for_path_param='', *args, **kwargs)`
:   Create and configure a DeepLynx configuration object.
    
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

    ### Ancestors (in MRO)

    * airflow.models.baseoperator.BaseOperator
    * airflow.models.abstractoperator.AbstractOperator
    * airflow.template.templater.Templater
    * airflow.utils.log.logging_mixin.LoggingMixin
    * airflow.models.taskmixin.DAGNode
    * airflow.models.taskmixin.DependencyMixin

    ### Class variables

    `template_fields`
    :

    ### Methods

    `execute(self, context)`
    :   Execute the configuration creation for DeepLynx.
        
        This method sets up the DeepLynx configuration based on the provided
        parameters, validates the input, and logs the configuration details.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
        
        Returns:
            None