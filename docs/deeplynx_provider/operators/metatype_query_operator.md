Module deeplynx_provider.operators.metatype_query_operator
==========================================================






Classes
-------

`MetatypeQueryOperator(metatype_name, container_id, write_to_file=False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Query all container data for a given metatype.
    
    This operator performs a GraphQL introspection query to retrieve the fields for a specified
    metatype, constructs a query using those fields, and then queries the metatype data. The
    result can either be written to a file or pushed to XCom.
    
    Attributes:
        metatype_name (str): The name of the metatype to query.
        container_id (str): The ID of the container to query.
        write_to_file (bool, optional): Flag indicating whether to write the result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize MetatypeQueryOperator with the given parameters.
    
    Args:
        metatype_name (str): The name of the metatype to query.
        container_id (str): The ID of the container to query.
        write_to_file (bool, optional): Flag indicating whether to write the result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
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
        
        This method performs a GraphQL introspection query to retrieve the fields for the specified
        metatype, constructs a query using those fields, and then queries the metatype data. The
        result is either written to a file or pushed to XCom.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None