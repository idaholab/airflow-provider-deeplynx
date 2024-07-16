Module deeplynx_provider.operators.import_container_operator
============================================================






Classes
-------

`ImportContainerOperator(container_id: str, file_path: str, dryrun: bool = False, import_ontology: bool = False, import_data_sources: bool = False, import_type_mappings: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Import another container's exported ontology, data sources, and type mappings.
    
    This operator requires a connection ID and the container_id to import container settings into.
    
    Attributes:
        container_id (str): The container_id to import into.
        file_path (str): The local path to the exported container that you want to import.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize ImportContainerOperator with the given parameters.
    
    Args:
        container_id (str): The container_id to import into.
        file_path (str): The local path to the exported container that you want to import.
        dryrun (bool, optional): Whether to perform a dry run of the import.
        import_ontology (bool, optional): Whether to import the ontology.
        import_data_sources (bool, optional): Whether to import the data sources.
        import_type_mappings (bool, optional): Whether to import the type mappings.
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
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None