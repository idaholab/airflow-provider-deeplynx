Module deeplynx_provider.operators.data_query_with_params_operator
==================================================================






Classes
-------

`DataQueryWithParamsOperator(query_type: deeplynx_provider.operators.query_helpers.QueryType, container_id: str, properties: dict, write_to_file: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Perform data queries using parameters for building the query body. This operator supports queries
    for graphs, metatypes, and relationships.
    
    Attributes:
        query_type (QueryType): The type of query to perform (GRAPH, METATYPE, RELATIONSHIP).
        container_id (str): The ID of the container in DeepLynx to query.
        properties (dict): A dictionary of properties used to build the query.
            The expected keys depend on the query_type:
                - GRAPH: {'root_node': str, 'depth': int}
                - METATYPE: {'metatype_name': str, 'fields_list': list}
                - RELATIONSHIP: {'relationship_name': str}
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize DataQueryWithParamsOperator with the given parameters.
    
    Args:
        query_type (QueryType): The type of query to perform.
        container_id (str): The ID of the container to query.
        properties (dict): A dictionary of properties used to build the query.
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
        
        This method generates the query, sends it to the DeepLynx API, processes the response,
        and either writes the result to a file or pushes it to XCom.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None

    `generate_query(self)`
    :   Generate the GraphQL query string based on the properties and query type.
        
        Returns:
            str: The generated GraphQL query string.

    `get_data_from_response(self, json_dict)`
    :   Extract data from the query response based on the query type.
        
        Args:
            json_dict (dict): The JSON response dictionary from the DeepLynx API.
        
        Returns:
            tuple: A tuple containing the data name and the extracted data.