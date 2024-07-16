from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from deeplynx_provider.operators.query_helpers import TimeSeriesQuery
import json

class TimeSeriesQueryOperator(DeepLynxBaseOperator):
    """
    Timeseries query with properties. This operator allows for specifying properties and
    query parameters to customize the timeseries query.

    Attributes:
        properties (list): A list of properties to include in the timeseries query.
        query_params (dict): A dictionary of query parameters to customize the timeseries query.
        container_id (str): The ID of the container in DeepLynx to query.
        data_source_id (str): The ID of the data source in DeepLynx to query.
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('properties', 'container_id', 'data_source_id', 'write_to_file')

    @apply_defaults
    def __init__(self, properties, query_params, container_id, data_source_id, write_to_file=False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize TimeSeriesQueryOperator with the given parameters.

        Args:
            properties (list): A list of properties to include in the timeseries query.
            query_params (dict): A dictionary of query parameters to customize the timeseries query.
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
        self.properties = properties
        self.query_params = query_params
        self.container_id = container_id
        self.data_source_id = data_source_id
        self.write_to_file = write_to_file

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method constructs and executes a timeseries query using the provided properties
        and query parameters, then processes the response.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        timeseries_api = deeplynx_hook.get_time_series_api()

        # Create query object
        query_obj = TimeSeriesQuery(self.properties, **self.query_params)
        query = query_obj.generate_query()
        body = {"query": query}

        # Get response
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
