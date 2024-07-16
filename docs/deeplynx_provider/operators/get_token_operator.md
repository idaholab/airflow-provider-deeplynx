Module deeplynx_provider.operators.get_token_operator
=====================================================






Classes
-------

`GetOauthTokenOperator(conn_id=None, host=None, api_key=None, api_secret=None, deeplynx_config: dict = None, expiry='1hr', *args, **kwargs)`
:   Retrieve an OAuth token from DeepLynx.
    
    This operator requires either a DeepLynx connection ID, or a host, API key, and secret.
    The token and host are pushed to XCom upon successful execution.
    
    Attributes:
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        api_key (str, optional): The API key for authentication.
        api_secret (str, optional): The API secret for authentication.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        expiry (str, optional): The expiry time for the token.
    
    Initialize GetOauthTokenOperator with the given parameters.
    
    Args:
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        api_key (str, optional): The API key for authentication.
        api_secret (str, optional): The API secret for authentication.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        expiry (str, optional): The expiry time for the token.
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

    `create_default_config(self)`
    :   Create a default DeepLynx configuration object.
        
        This method creates a default DeepLynx configuration object with verify_ssl set to False.
        
        Returns:
            deep_lynx.Configuration: The default DeepLynx configuration object.

    `execute(self, context)`
    :   Execute the process to retrieve an OAuth token from DeepLynx.
        
        This method processes the input parameters, retrieves the OAuth token using the
        DeepLynx API, and pushes the token and host to XCom.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
        
        Returns:
            None

    `set_class_variable_values(self)`
    :   Process the input parameters and set class variables accordingly.
        
        This method processes the input parameters based on the provided conn_id, host,
        api_key, api_secret, and deeplynx_config, and creates a default configuration if needed.
        
        input processing logic:
        1. If conn_id is provided:
           - Fetch API key, secret, and optionally host from the Airflow connection.
           - Create a default deeplynx_config with verify_ssl set to False.
        2. If deeplynx_config is provided:
           - Use the provided deeplynx_config object.
           - Ensure API key and secret are provided.
        3. If neither deeplynx_config nor conn_id is provided:
           - Ensure host, API key, and secret are provided.
           - Create a default deeplynx_config with verify_ssl set to False.
        
        Returns:
            deep_lynx.Configuration: The DeepLynx configuration object.
        
        Raises:
            AirflowException: If the parameter validation fails.

    `validate_params(self)`
    :   Validate the input parameters to ensure correctness.
        
        This method checks that only one of conn_id, host, or deeplynx_config is provided,
        and that either conn_id or api_key and api_secret are provided.
        
        Raises:
            AirflowException: If the parameter validation fails.