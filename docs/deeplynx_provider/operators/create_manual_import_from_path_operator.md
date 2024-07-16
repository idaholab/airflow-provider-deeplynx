Module deeplynx_provider.operators.create_manual_import_from_path_operator
==========================================================================






Classes
-------

`CreateManualImportFromPathOperator(container_id: str, data_source_id: str, file_path: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Operator to create a manual import in DeepLynx; file import using file path.
    
    Attributes:
        container_id (str): ID of the container in DeepLynx.
        data_source_id (str): ID of the data source in DeepLynx.
        file_path (str): Local file path string.
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
    :   Custom logic to create a manual import in DeepLynx.
        
        Args:
            context (dict): Airflow context dictionary.
            deeplynx_hook (DeepLynxHook): Hook to interact with DeepLynx API.
        
        Raises:
            AirflowException: If the API call to create the manual import fails.