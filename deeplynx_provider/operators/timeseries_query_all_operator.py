from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, TimeSeriesQuery

class TimeSeriesQueryAllOperator(BaseOperator):
    # By adding template_fields, the specified fields will be automatically templated by Airflow when the DAG is executed. This means that any template variables used in these fields, like {{ params.CONTAINER_ID }}, will be appropriately replaced with their corresponding values from the DAG's params or other sources like XCom.
    template_fields = ('container_id', 'datasource_id', 'token', 'file_path')

    @apply_defaults
    def __init__(self, conn_id, container_id, datasource_id, token, file_path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.container_id = container_id
        self.datasource_id = datasource_id
        self.token = token
        self.file_path = file_path

    def execute(self, context):
        import os
        import json
        import deep_lynx
        from deeplynx_provider.hooks.deeplynx import DeepLynxHook
        from deep_lynx.api.time_series_api import TimeSeriesApi

        ### get timeseries_api
        hook = DeepLynxHook(self.conn_id)
        client = hook.get_client(self.token)
        timeseries_api = TimeSeriesApi(client)

        ### first, instrospection query
        introspection_query = GraphQLIntrospectionQuery("Timeseries").generate_query()
        introspection_response = timeseries_api.timeseries_data_source_query({"query": introspection_query}, self.container_id, self.datasource_id)
        fields_list = IntrospectionQueryResponseToFieldsList(introspection_response)

        ### now query using fields_list from introspection_query
        query_obj = TimeSeriesQuery(fields_list)
        query = query_obj.generate_query()
        body = {"query": query}
        response = timeseries_api.timeseries_data_source_query(body, self.container_id, self.datasource_id)
        # Accessing the Timeseries data
        response_data = response.to_dict()
        timeseries_data = response_data['data']['Timeseries']
        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
        # Write response to a file
        with open(self.file_path, 'w') as f:
            json.dump(timeseries_data, f)

        return {
            'file_path': "self.file_path"
        }
