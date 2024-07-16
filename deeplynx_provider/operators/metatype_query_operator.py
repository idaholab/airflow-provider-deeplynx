# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deeplynx_provider.operators.query_helpers import GraphQLIntrospectionQuery, IntrospectionQueryResponseToFieldsList, validate_introspection_response, MetatypeQuery
import deep_lynx
from deep_lynx.api.data_query_api import DataQueryApi
import json

class MetatypeQueryOperator(DeepLynxBaseOperator):
    """
    Query all container data for a given metatype.

    This operator performs a GraphQL introspection query to retrieve the fields for a specified
    metatype, constructs a query using those fields, and then queries the metatype data. The
    result can either be written to a file or pushed to XCom.

    Attributes:
        metatype_name (str): The name of the metatype to query.
        container_id (str): The ID of the container to query.
        write_to_file (bool, optional): Flag indicating whether to write the result to a file.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('metatype_name', 'container_id', 'write_to_file')

    @apply_defaults
    def __init__(self, metatype_name, container_id, write_to_file=False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize MetatypeQueryOperator with the given parameters.

        Args:
            metatype_name (str): The name of the metatype to query.
            container_id (str): The ID of the container to query.
            write_to_file (bool, optional): Flag indicating whether to write the result to a file.
            conn_id (str, optional): The connection ID to use for the operation.
            host (str, optional): The host for the DeepLynx API.
            deeplynx_config (dict, optional): Additional configuration for DeepLynx.
            token (str, optional): The token for authentication.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.metatype_name = metatype_name
        self.container_id = container_id
        self.write_to_file = write_to_file

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method performs a GraphQL introspection query to retrieve the fields for the specified
        metatype, constructs a query using those fields, and then queries the metatype data. The
        result is either written to a file or pushed to XCom.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        data_query_api = deeplynx_hook.get_data_query_api()

        # Perform introspection query
        introspection_query = GraphQLIntrospectionQuery(self.metatype_name).generate_query()
        introspection_response = data_query_api.data_query({"query": introspection_query}, self.container_id)
        fields_list = IntrospectionQueryResponseToFieldsList(introspection_response, self.metatype_name)

        # Construct and execute the metatype query using the fields from introspection
        query_obj = MetatypeQuery(self.metatype_name, fields_list)
        query = query_obj.generate_query()
        body = {"query": query}
        response = data_query_api.data_query(body, self.container_id)

        # Accessing the metatype data
        response_data = response.to_dict()
        metatype_data = response_data['data']['metatypes'][self.metatype_name]

        # Format data as a JSON string
        metatype_json_data = json.dumps(metatype_data, indent=4)

        # Get data filename
        data_filename = self.format_query_response_filename(context, self.metatype_name)

        # Write or push to XCom
        self.write_or_push_to_xcom(context, metatype_json_data, data_filename)
