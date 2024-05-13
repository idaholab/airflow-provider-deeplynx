from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
# 
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, MetatypeQuery


class MetatypeQueryOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('metatype_name', 'container_id', 'file_path', 'write_to_file')

    @apply_defaults
    def __init__(self, metatype_name, container_id, file_path=None, write_to_file=True, host: str = None, deep_lynx_config: Configuration = None, token: str = None, *args, **kwargs):
        super().__init__(host=host, deep_lynx_config=deep_lynx_config, token=token, *args, **kwargs)
        self.metatype_name = metatype_name
        self.container_id = container_id
        self.file_path = file_path
        self.write_to_file = write_to_file
        if self.write_to_file and not self.file_path:
            raise ValueError("file_path must be provided if write_to_file is True")

    def do_custom_logic(self, context, deeplynx_hook):
        ### get api client
        data_query_api = deeplynx_hook.get_data_query_api()
        ### first, instrospection query
        introspection_query = GraphQLIntrospectionQuery(self.metatype_name).generate_query()
        introspection_response = data_query_api.data_query({"query": introspection_query}, self.container_id)
        fields_list = IntrospectionQueryResponseToFieldsList(introspection_response)
        ### now query using fields_list from introspection_query
        query_obj = MetatypeQuery(self.metatype_name, fields_list)
        query = query_obj.generate_query()
        body = {"query": query}
        response = data_query_api.data_query(body, self.container_id)
        ### Accessing the Metatype data
        response_data = response.to_dict()
        metatype_data = response_data['data']['metatypes'][self.metatype_name]
        #
        self.write_or_push_to_xcom(context, metatype_data, self.file_path, self.write_to_file)
