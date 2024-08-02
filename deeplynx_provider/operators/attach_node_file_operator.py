from airflow.utils.decorators import apply_defaults
import logging
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
#
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, MetatypeQuery


class AttachFileOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'node', 'file_id')

    @apply_defaults
    def __init__(self, container_id: str, node: str, file_id: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize AttachFileOperator with the given parameters.

        Args:
            container_id (str): The ID of the container in DeepLynx.
            node (str): The ID of the node in DeepLynx.
            file_id (str): The ID of the file to attach.
            conn_id (str, optional): The connection ID to use for the operation.
            host (str, optional): The host for the DeepLynx API.
            deeplynx_config (dict, optional): Additional configuration for DeepLynx.
            token (str, optional): The token for authentication.
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.node = node
        self.file_id = file_id
        

    def do_custom_logic(self, context, deeplynx_hook):
        ### get api client
        graph_api = deeplynx_hook.get_graph_api()
        ### download_file
        try:
            graph_api.attach_node_file(container_id=self.container_id, node_id=self.node, file_id=self.file_id)
            ### push downloaded file location to xcom
            task_instance = context['task_instance']
            task_instance.xcom_push(key='sucess', value=True)
        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            raise