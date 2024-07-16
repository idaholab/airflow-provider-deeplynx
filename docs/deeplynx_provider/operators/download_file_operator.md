Module deeplynx_provider.operators.download_file_operator
=========================================================






Classes
-------

`DownloadFileOperator(container_id: str, file_id: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Download a file from a specified container in DeepLynx.
    
    This operator requires the container ID and file ID, along with optional parameters
    for connection details and additional DeepLynx configuration.
    
    Attributes:
        container_id (str): The ID of the container from which to download the file.
        file_id (str): The ID of the file to download.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize DownloadFileOperator with the given parameters.
    
    Args:
        container_id (str): The ID of the container from which to download the file.
        file_id (str): The ID of the file to download.
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
        
        This method downloads a file from the specified container using the DeepLynx API,
        and pushes the downloaded file location to XCom.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None