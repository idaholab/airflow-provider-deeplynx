from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deeplynx_provider.operators.query_helpers import QueryType
import deep_lynx
from deep_lynx.api.data_query_api import DataQueryApi
import json
from enum import Enum

class DataQueryOperator(DeepLynxBaseOperator):
    """
    Perform data queries using a predefined query body. This operator supports queries
    for graphs, metatypes, and relationships.

    Attributes:
        query_type (QueryType): The type of query to perform (GRAPH, METATYPE, RELATIONSHIP).
        container_id (str): The ID of the container in DeepLynx to query.
        query_body (str): The GraphQL query body to execute.
        write_to_file (bool, optional): Whether to write the query result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + (
        'query_type', 'query_body', 'container_id', 'write_to_file'
    )

    @apply_defaults
    def __init__(self, query_type: QueryType, container_id: str, query_body: str, write_to_file: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize DataQueryOperator with the given parameters.

        Args:
            query_type (QueryType): The type of query to perform.
            container_id (str): The ID of the container to query.
            query_body (str): The GraphQL query body to execute.
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
        self.query_body = query_body
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

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method sends the query to the DeepLynx API, processes the response,
        and either writes the result to a file or pushes it to XCom.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        data_query_api = deeplynx_hook.get_data_query_api()

        # Use provided query body
        body = {"query": self.query_body}
        response = data_query_api.data_query(body, self.container_id)

        (data_name, response_data) = self.get_data_from_response(response.to_dict())
        response_data_json = json.dumps(response_data, indent=4)

        # Get data filename
        data_filename = self.format_query_response_filename(context, data_name)

        # Write or push to XCom
        self.write_or_push_to_xcom(context, response_data_json, data_filename)
