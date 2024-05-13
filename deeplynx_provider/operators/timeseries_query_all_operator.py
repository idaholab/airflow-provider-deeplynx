from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
#
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, TimeSeriesQuery

# TODO: Overlap with TimeSeriesQueryOperator and MetatypeQueryOperator that should probably be consolidated
class TimeSeriesQueryAllOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'file_path', 'write_to_file')

    @apply_defaults
    def __init__(self, container_id, datasource_id, file_path=None, write_to_file=True, host=None, deep_lynx_config=None, token=None, *args, **kwargs):
        super().__init__(host=host, deep_lynx_config=deep_lynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.datasource_id = datasource_id
        self.file_path = file_path
        self.write_to_file = write_to_file
        if self.write_to_file and not self.file_path:
            raise ValueError("file_path must be provided if write_to_file is True")

    def do_custom_logic(self, context, deeplynx_hook):
        ### get api client
        timeseries_api = deeplynx_hook.get_time_series_api()
        ### first, instrospection query
        introspection_query = GraphQLIntrospectionQuery("Timeseries").generate_query()
        introspection_response = timeseries_api.timeseries_data_source_query({"query": introspection_query}, self.container_id, self.datasource_id)
        fields_list = IntrospectionQueryResponseToFieldsList(introspection_response)
        ### now query using fields_list from introspection_query
        query_obj = TimeSeriesQuery(fields_list)
        query = query_obj.generate_query()
        body = {"query": query}
        response = timeseries_api.timeseries_data_source_query(body, self.container_id, self.datasource_id)
        ### Accessing the Timeseries data
        response_data = response.to_dict()
        timeseries_data = response_data['data']['Timeseries']
        #
        self.write_or_push_to_xcom(context, timeseries_data, self.file_path, self.write_to_file)
