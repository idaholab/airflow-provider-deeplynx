Module deeplynx_provider.operators.set_type_mapping_active_operator
===================================================================






Classes
-------

`SetTypeMappingActiveOperator(container_id: str, data_source_id: str, match_keys: list[str], conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Set a type mapping as active in DeepLynx.
    
    This is meant to be used with a type mapping that already has transformations made and needs to be set active.
    This is convenient if you're importing type mappings and transformations into a container.
    
    This operator requires the container ID, data source ID, and match keys to find the
    appropriate type mapping and set it as active. It only looks at the keys and not the values' data types.
    If your data source happens to have two or more type mappings with the exact same set of keys, but different values' data types, this operator might not work for you.
    If we want to support automated type mapping in Airflow, we need to develop a more complete solution.
    
    Attributes:
        container_id (str): The ID of the container.
        data_source_id (str): The ID of the data source.
        match_keys (list of str): A list of keys to match in the sample payload of type mappings.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize SetTypeMappingActiveOperator with the given parameters.
    
    Args:
        container_id (str): The ID of the container.
        data_source_id (str): The ID of the data source.
        match_keys (list of str): A list of keys to match in the sample payload of type mappings.
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
        
        This method retrieves type mappings for the specified data source, finds the first
        type mapping with a sample payload matching the provided keys, and sets it as active.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None