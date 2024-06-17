from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, MetatypeQuery
import deep_lynx
from deep_lynx.api.data_query_api import DataQueryApi
import json

class MetatypeQueryOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('metatype_name', 'container_id', 'write_to_file')

    @apply_defaults
    def __init__(self, metatype_name, container_id, write_to_file=False, conn_id:str=None, host:str=None, deeplynx_config:dict=None, token:str=None, *args, **kwargs):
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.metatype_name = metatype_name
        self.container_id = container_id
        self.write_to_file = write_to_file

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

        ## Format data as JSON string
        metatype_json_data = json.dumps(metatype_data, indent=4)

        ## get data_filename
        data_filename = self.format_query_response_filename(context, self.metatype_name)

        ##
        self.write_or_push_to_xcom(context, metatype_json_data, data_filename)
