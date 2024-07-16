from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from airflow.exceptions import AirflowException

class UploadFileOperator(DeepLynxBaseOperator):
    """
    Upload a file to a specified data source in DeepLynx.
    The operator uploads the file and pushes the file ID to XCom upon success.

    Attributes:
        container_id (str): The ID of the container in DeepLynx.
        data_source_id (str): The ID of the data source in DeepLynx.
        file_path (str): The path of the file to upload.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'file_path')

    @apply_defaults
    def __init__(self, container_id: str, data_source_id: str, file_path: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
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
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.data_source_id = data_source_id
        self.file_path = file_path

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method uploads the specified file to the given container and data source,
        then pushes the file ID to XCom upon success.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        data_sources_api = deeplynx_hook.get_data_sources_api()

        # Upload file
        response = data_sources_api.upload_file(self.container_id, self.data_source_id, file=self.file_path)

        # Check if the response indicates success; push file_id to xcom if success
        if response.get('isError') != False:
            raise AirflowException(f"Failed to upload file: {response}")
        elif response.get('isError') == False:
            file_id = response.get('value')[0].get('value').get('id')
            task_instance = context['task_instance']
            task_instance.xcom_push(key='file_id', value=file_id)
