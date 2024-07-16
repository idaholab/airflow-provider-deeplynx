Module deeplynx_provider.operators.set_data_source_active_operator
==================================================================






Classes
-------

`SetDataSourceActiveOperator(container_id: str, data_source_id: str = None, data_source_name: str = None, timeseries: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Operator to set a data source active based on the data_source_id. If data_source_name is supplied,
    will search for a data source with the same name and set active if found.
    
    Attributes:
        container_id (str): ID of the container in DeepLynx.
        data_source_id (str, optional): ID of the data source in DeepLynx. Defaults to None.
        data_source_name (str, optional): Name of the data source in DeepLynx. Defaults to None.
        timeseries (bool, optional): Indicates if the data source is timeseries. Defaults to False.
        conn_id (str, optional): Connection ID for DeepLynx. Defaults to None.
        host (str, optional): Host for DeepLynx. Defaults to None.
        deeplynx_config (dict, optional): Configuration dictionary for DeepLynx. Defaults to None.
        token (str, optional): Token for authentication. Defaults to None.
    
    Initialize DeepLynxBaseOperator with the given parameters.
    
    Args:
        token (str, optional): The token for authentication.
        conn_id (str, optional): The connection ID to use.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    ### Ancestors (in MRO)

    * deeplynx_provider.operators.deeplynx_base_operator.DeepLynxBaseOperator
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

    `do_custom_logic(self, context, deeplynx_hook)`
    :   Custom logic to set a data source active in DeepLynx.
        
        Args:
            context (dict): Airflow context dictionary.
            deeplynx_hook (DeepLynxHook): Hook to interact with DeepLynx API.
        
        Raises:
            AirflowException: If the API call to set the data source active fails.

    `find_id_by_name(self, response, target_name)`
    :   Processes the list_data_sources response to find the id of an object by its name.
        
        Args:
            response (dict): The response object to be processed (as a dictionary).
            target_name (str): The name to be searched for in the response.
        
        Returns:
            str: The id of the object with the matching name, or None if not found.
        
        Raises:
            ValueError: If the response does not meet expected structure.