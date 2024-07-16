from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration


class UploadFileOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'file_path')

    @apply_defaults
    def __init__(self, container_id: str, data_source_id: str, file_path: str, conn_id: str = None, host:str=None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.data_source_id = data_source_id
        self.file_path = file_path

    def do_custom_logic(self, context, deeplynx_hook):
        ### get api client
        data_sources_api = deeplynx_hook.get_data_sources_api()
        ### upload_file
        response = data_sources_api.upload_file(self.container_id, self.data_source_id, file = self.file_path)
        ### Check if the response indicates success; push file_id to xcom if success
        if response.get('isError') != False:
            raise AirflowException(f"Failed to create manual import: {response}")
        elif response.get('isError') == False:
            file_id = response.get('value')[0].get('value').get('id')
            task_instance = context['task_instance']
            task_instance.xcom_push(key='file_id', value=file_id)
