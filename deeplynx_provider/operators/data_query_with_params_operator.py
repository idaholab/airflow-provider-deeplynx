from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deeplynx_provider.operators.query_helpers import (
    MetatypeQuery,
    TimeSeriesQuery,
    RelationshipQuery,
    GraphQuery,
    QueryType
)
import deep_lynx
from deep_lynx.api.data_query_api import DataQueryApi
import json

class DataQueryWithParamsOperator(DeepLynxBaseOperator):
    """
    DataQueryWithParamsOperator is an Airflow operator to perform data queries
    using parameters for building the query body. This operator supports queries
    for graphs, metatypes, and relationships.

    Attributes:
        query_type (QueryType): The type of query to perform (GRAPH, METATYPE, RELATIONSHIP).
        container_id (str): The ID of the container in DeepLynx to query.
        properties (dict): A dictionary of properties used to build the query.
            The expected keys depend on the query_type:
                - GRAPH: {'root_node': str, 'depth': int}
                - METATYPE: {'metatype_name': str, 'fields_list': list}
                - RELATIONSHIP: {'relationship_name': str}
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + (
        'query_type', 'properties', 'container_id', 'write_to_file'
    )

    @apply_defaults
    def __init__(self, query_type: QueryType, container_id: str, properties: dict, write_to_file: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize DataQueryWithParamsOperator with the given parameters.

        Args:
            query_type (QueryType): The type of query to perform.
            container_id (str): The ID of the container to query.
            properties (dict): A dictionary of properties used to build the query.
            write_to_file (bool, optional): Whether to write the query result to a file.
            conn_id (str, optional): The connection ID to use.
            host (str, optional): The host for the DeepLynx API.
            deeplynx_config (dict, optional): Additional configuration for DeepLynx.
            token (str, optional): The token for authentication.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.query_type = query_type
        self.properties = properties
        self.container_id = container_id
        self.write_to_file = write_to_file

    def get_data_from_response(self, json_dict):
        """
        Extract data from the query response based on the query type.

        Args:
            json_dict (dict): The JSON response dictionary from the DeepLynx API.

        Returns:
            tuple: A tuple containing the data name and the extracted data.
        """
        if self.query_type == QueryType.GRAPH:
            graph = json_dict["data"]["graph"]
            return ("graph", graph)
        elif self.query_type == QueryType.METATYPE:
            metatypes = json_dict["data"]["metatypes"]
            metatype_name = next(iter(metatypes))
            metatype_values = metatypes[metatype_name]
            return (metatype_name, metatype_values)
        elif self.query_type == QueryType.RELATIONSHIP:
            relationships = json_dict["data"]["relationships"]
            relationship_name = next(iter(relationships))
            relationship_values = relationships[relationship_name]
            return (relationship_name, relationship_values)
        else:
            raise ValueError("Invalid query type")

    def generate_query(self):
        """
        Generate the GraphQL query string based on the properties and query type.

        Returns:
            str: The generated GraphQL query string.
        """
        if self.query_type == QueryType.GRAPH:
            query_obj = GraphQuery(self.properties.get('root_node'), self.properties.get('depth'))
        elif self.query_type == QueryType.METATYPE:
            query_obj = MetatypeQuery(self.properties.get('metatype_name'), self.properties.get('fields_list'))
        elif self.query_type == QueryType.RELATIONSHIP:
            query_obj = RelationshipQuery(self.properties.get('relationship_name'))
        else:
            raise ValueError("Invalid query type")

        return query_obj.generate_query()

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method generates the query, sends it to the DeepLynx API, processes the response,
        and either writes the result to a file or pushes it to XCom.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        data_query_api = deeplynx_hook.get_data_query_api()

        # Generate query using provided properties
        query = self.generate_query()
        body = {"query": query}
        response = data_query_api.data_query(body, self.container_id)

        (data_name, response_data) = self.get_data_from_response(response.to_dict())
        response_data_json = json.dumps(response_data, indent=4)

        # Get data filename
        data_filename = self.format_query_response_filename(context, data_name)

        # Write or push to XCom
        self.write_or_push_to_xcom(context, response_data_json, data_filename)
