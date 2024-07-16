Module deeplynx_provider.operators.timeseries_query_operator
============================================================






Classes
-------

`TimeSeriesQueryOperator(properties, query_params, container_id, data_source_id, write_to_file=False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Timeseries query with properties. This operator allows for specifying properties and
    query parameters to customize the timeseries query.
    
    Attributes:
        properties (list): A list of properties to include in the timeseries query.
        query_params (dict): A dictionary of query parameters to customize the timeseries query.
        container_id (str): The ID of the container in DeepLynx to query.
        data_source_id (str): The ID of the data source in DeepLynx to query.
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize TimeSeriesQueryOperator with the given parameters.
    
    Args:
        properties (list): A list of properties to include in the timeseries query.
        query_params (dict): A dictionary of query parameters to customize the timeseries query.
        container_id (str): The ID of the container to query.
        data_source_id (str): The ID of the data source to query.
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
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
    :   Execute the custom logic for the operator.
        
        This method constructs and executes a timeseries query using the provided properties
        and query parameters, then processes the response.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None