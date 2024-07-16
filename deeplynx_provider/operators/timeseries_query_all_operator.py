from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, TimeSeriesQuery
import json

class TimeSeriesQueryAllOperator(DeepLynxBaseOperator):
    """
    Query all timeseries data for a given datasouce.
    This operator performs an introspection query to retrieve
    all available fields and then executes a timeseries query using those fields.

    Attributes:
        container_id (str): The ID of the container in DeepLynx to query.
        data_source_id (str): The ID of the data source in DeepLynx to query.
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'write_to_file')

    @apply_defaults
    def __init__(self, container_id, data_source_id, write_to_file=False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize TimeSeriesQueryAllOperator with the given parameters.

        Args:
            container_id (str): The ID of the container to query.
            data_source_id (str): The ID of the data source to query.
            write_to_file (bool, optional): Whether to write the query result to a file.
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
        self.write_to_file = write_to_file

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method performs an introspection query to retrieve all available fields for the timeseries,
        then constructs and executes a timeseries query using those fields.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        timeseries_api = deeplynx_hook.get_time_series_api()

        # Perform introspection query
        introspection_query = GraphQLIntrospectionQuery("Timeseries").generate_query()
        introspection_response = timeseries_api.timeseries_data_source_query(
            {"query": introspection_query}, self.container_id, self.data_source_id)
        fields_list = IntrospectionQueryResponseToFieldsList(introspection_response, "Timeseries")

        # Perform timeseries query using fields_list from introspection query
        query_obj = TimeSeriesQuery(fields_list)
        query = query_obj.generate_query()
        body = {"query": query}
        response = timeseries_api.timeseries_data_source_query(body, self.container_id, self.data_source_id)

        # Accessing the Timeseries data
        response_data = response.to_dict()
        timeseries_data = response_data['data']['Timeseries']

        # Format data as JSON string
        json_data = json.dumps(timeseries_data, indent=4)

        # Get data filename
        data_filename = self.format_query_response_filename(context, 'Timeseries' + self.container_id + '_' + self.data_source_id)

        # Write or push to XCom
        self.write_or_push_to_xcom(context, json_data, data_filename)
