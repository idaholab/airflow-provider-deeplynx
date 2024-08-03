Module deeplynx_provider.hooks.deeplynx
=======================================






Classes
-------

`DeepLynxHook(deeplynx_config: deep_lynx.configuration.Configuration, token: str = None)`
:   DeepLynxHook is an Airflow hook to interact with the DeepLynx API.
    
    This hook initializes the DeepLynx client with the provided configuration and token,
    and provides methods to access various DeepLynx APIs.
    
    Attributes:
        conn_name_attr (str): The name attribute for the DeepLynx connection.
        default_conn_name (str): The default connection name for DeepLynx.
        conn_type (str): The connection type for DeepLynx.
        hook_name (str): The name of the hook.
        deeplynx_config (Configuration): The configuration object for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize DeepLynxHook with the given configuration and token.
    
    Args:
        deeplynx_config (Configuration): The configuration object for DeepLynx.
        token (str, optional): The token for authentication.

    ### Ancestors (in MRO)

    * airflow.hooks.base.BaseHook
    * airflow.utils.log.logging_mixin.LoggingMixin

    ### Class variables

    `conn_name_attr`
    :

    `conn_type`
    :

    `default_conn_name`
    :

    `hook_name`
    :

    ### Static methods

    `get_ui_field_behaviour() ‑> Dict`
    :   Returns custom field behaviour for the connection form.
        
        This method customizes the visibility and labels of the fields in the connection form.
        
        Returns:
            Dict: The custom field behaviour configuration.

    ### Methods

    `get_authentication_api(self)`
    :   Get the Authentication API client.
        
        Returns:
            deep_lynx.AuthenticationApi: The Authentication API client.

    `get_containers_api(self)`
    :   Get the Containers API client.
        
        Returns:
            deep_lynx.ContainersApi: The Containers API client.

    `get_data_query_api(self)`
    :   Get the DataQuery API client.
        
        Returns:
            deep_lynx.DataQueryApi: The DataQuery API client.

    `get_data_sources_api(self)`
    :   Get the DataSources API client.
        
        Returns:
            deep_lynx.DataSourcesApi: The DataSources API client.

    `get_deep_lynx_client(self, bearerAuth=True)`
    :   Create a new DeepLynx client and set the authorization token.
        
        Args:
            bearerAuth (bool, optional): Whether to use Bearer authentication. Defaults to True.
        
        Returns:
            deep_lynx.ApiClient: The initialized DeepLynx API client.

    `get_graph_api(self)`
    :   Get the Graph API client.
        
        Returns:
            deep_lynx.GraphApi: The Graph API client.

    `get_time_series_api(self)`
    :   Get the TimeSeries API client.
        
        Returns:
            deep_lynx.TimeSeriesApi: The TimeSeries API client.

    `get_type_mapping_api(self)`
    :   Get the DataTypeMappings API client.
        
        Returns:
            deep_lynx.DataTypeMappingsApi: The DataTypeMappings API client.