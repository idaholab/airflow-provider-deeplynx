from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
#
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, MetatypeQuery


class DownloadFileOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'file_id')

    @apply_defaults
    def __init__(self, container_id: str, file_id: str, conn_id: str = None, deeplynx_config: dict = None, token: str = None, minio_uri: str = None, *args, **kwargs):
        super().__init__(conn_id=conn_id, deeplynx_config=deeplynx_config, token=token, minio_uri=minio_uri, *args, **kwargs)
        self.container_id = container_id
        self.file_id = file_id

    def do_custom_logic(self, context, deeplynx_hook):
        ### get api client
        data_sources_api = deeplynx_hook.get_data_sources_api()
        ### download_file
        response = data_sources_api.download_file(self.container_id, self.file_id)
        ### push downloaded file location to xcom
        task_instance = context['task_instance']
        task_instance.xcom_push(key='downloaded_file_path', value=response)
