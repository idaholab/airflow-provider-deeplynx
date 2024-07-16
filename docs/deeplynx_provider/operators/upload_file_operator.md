Module deeplynx_provider.operators.upload_file_operator
=======================================================






Classes
-------

`UploadFileOperator(container_id: str, data_source_id: str, file_path: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Upload a file to a specified data source in DeepLynx.
    The operator uploads the file and pushes the file ID to XCom upon success.
    
    Attributes:
        container_id (str): The ID of the container in DeepLynx.
        data_source_id (str): The ID of the data source in DeepLynx.
        file_path (str): The path of the file to upload.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize UploadFileOperator with the given parameters.
    
    Args:
        container_id (str): The ID of the container in DeepLynx.
        data_source_id (str): The ID of the data source in DeepLynx.
        file_path (str): The path of the file to upload.
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
        
        This method uploads the specified file to the given container and data source,
        then pushes the file ID to XCom upon success.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None