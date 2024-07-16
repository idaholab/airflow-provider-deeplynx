from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration

class DownloadFileOperator(DeepLynxBaseOperator):
    """
    Download a file from a specified container in DeepLynx.

    This operator requires the container ID and file ID, along with optional parameters
    for connection details and additional DeepLynx configuration.

    Attributes:
        container_id (str): The ID of the container from which to download the file.
        file_id (str): The ID of the file to download.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'file_id')

    @apply_defaults
    def __init__(self, container_id: str, file_id: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
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
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.file_id = file_id

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method downloads a file from the specified container using the DeepLynx API,
        and pushes the downloaded file location to XCom.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """

        # Get API client
        data_sources_api = deeplynx_hook.get_data_sources_api()

        # Download file
        response = data_sources_api.download_file(self.container_id, self.file_id)

        # Push downloaded file location to XCom
        task_instance = context['task_instance']
        task_instance.xcom_push(key='downloaded_file_path', value=response)
