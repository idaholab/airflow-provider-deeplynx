Module deeplynx_provider.operators.data_query_operator
======================================================






Classes
-------

`DataQueryOperator(query_type: deeplynx_provider.operators.query_helpers.QueryType, container_id: str, query_body: str, write_to_file: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Perform data queries using a predefined query body. This operator supports queries
    for graphs, metatypes, and relationships.
    
    Attributes:
        query_type (QueryType): The type of query to perform (GRAPH, METATYPE, RELATIONSHIP).
        container_id (str): The ID of the container in DeepLynx to query.
        query_body (str): The GraphQL query body to execute.
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize DataQueryOperator with the given parameters.
    
    Args:
        query_type (QueryType): The type of query to perform.
        container_id (str): The ID of the container to query.
        query_body (str): The GraphQL query body to execute.
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
        
        This method sends the query to the DeepLynx API, processes the response,
        and either writes the result to a file or pushes it to XCom.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None

    `get_data_from_response(self, json_dict)`
    :   Extract data from the query response based on the query type.
        
        Args:
            json_dict (dict): The JSON response dictionary from the DeepLynx API.
        
        Returns:
            tuple: A tuple containing the data name and the extracted data.