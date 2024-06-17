from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from deeplynx_provider.operators.query_helpers import TimeSeriesQuery
import json

class TimeSeriesQueryOperator(DeepLynxBaseOperator):
    # extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('properties', 'container_id', 'datasource_id', 'write_to_file')

    @apply_defaults
    def __init__(self, properties, query_params, container_id, datasource_id, write_to_file=False, conn_id:str=None, host:str=None, deeplynx_config:dict=None, token:str=None, *args, **kwargs):
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.properties = properties
        self.query_params = query_params
        self.container_id = container_id
        self.datasource_id = datasource_id
        self.write_to_file = write_to_file

    def do_custom_logic(self, context, deeplynx_hook):
        ### get api client
        timeseries_api = deeplynx_hook.get_time_series_api()

        ### create query object
        query_obj = TimeSeriesQuery(self.properties, **self.query_params)
        query = query_obj.generate_query()
        body = {"query": query}

        ### get response
        response = timeseries_api.timeseries_data_source_query(body, self.container_id, self.datasource_id)

        ### Accessing the Timeseries data
        response_data = response.to_dict()
        timeseries_data = response_data['data']['Timeseries']

        ## Format data as JSON string
        json_data = json.dumps(timeseries_data, indent=4)

        ## get data_filename
        data_filename = self.format_query_response_filename(context, 'Timeseries' + self.container_id + '_' + self.datasource_id)

        ##
        self.write_or_push_to_xcom(context, json_data, data_filename)
