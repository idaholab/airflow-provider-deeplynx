from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, MetatypeQuery

class MetatypeQueryOperator(BaseOperator):
    # By adding template_fields, the specified fields will be automatically templated by Airflow when the DAG is executed. This means that any template variables used in these fields, like {{ params.CONTAINER_ID }}, will be appropriately replaced with their corresponding values from the DAG's params or other sources like XCom.
    template_fields = ('metatype_name', 'container_id', 'token', 'file_path')

    @apply_defaults
    def __init__(self, conn_id, metatype_name, container_id, token, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.metatype_name = metatype_name
        self.container_id = container_id
        self.token = token
        self.file_path = file_path

    def execute(self, context):
        import os
        import json
        import deep_lynx
        from deeplynx_provider.hooks.deeplynx import DeepLynxHook
        from deep_lynx.api.data_query_api import DataQueryApi

        ### create an instance of the API class
        hook = DeepLynxHook(self.conn_id)
        client = hook.get_client(self.token)
        graph = DataQueryApi(client)

        ### first, instrospection query
        introspection_query = GraphQLIntrospectionQuery(self.metatype_name).generate_query()
        introspection_response = graph.data_query({"query": introspection_query}, self.container_id)
        fields_list = IntrospectionQueryResponseToFieldsList(introspection_response)

        ### now query using fields_list from introspection_query
        query_obj = MetatypeQuery(self.metatype_name, fields_list)
        query = query_obj.generate_query()
        body = {"query": query}
        response = graph.data_query(body, self.container_id)
        # Accessing the Metatype data
        response_data = response.to_dict()
        metatype_data = response_data['data']['metatypes'][self.metatype_name]
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        # Write response to a file
        with open(self.file_path, 'w') as f:
            json.dump(metatype_data, f)

        return {
            'file_path': "self.file_path"
        }
