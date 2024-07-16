# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from airflow.exceptions import AirflowException
from pprint import pprint

class CreateManualImportFromPathOperator(DeepLynxBaseOperator):
    """
    Operator to create a manual import in DeepLynx; file import using file path.

    Attributes:
        container_id (str): ID of the container in DeepLynx.
        data_source_id (str): ID of the data source in DeepLynx.
        file_path (str): Local file path string.
        conn_id (str, optional): Connection ID for DeepLynx. Defaults to None.
        host (str, optional): Host for DeepLynx. Defaults to None.
        deeplynx_config (dict, optional): Configuration dictionary for DeepLynx. Defaults to None.
        token (str, optional): Token for authentication. Defaults to None.
    """
    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'file_path')

    @apply_defaults
    def __init__(self, container_id: str, data_source_id: str, file_path: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.data_source_id = data_source_id
        self.file_path = file_path

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Custom logic to create a manual import in DeepLynx.

        Args:
            context (dict): Airflow context dictionary.
            deeplynx_hook (DeepLynxHook): Hook to interact with DeepLynx API.

        Raises:
            AirflowException: If the API call to create the manual import fails.
        """
        try:
            # Get API client
            data_sources_api = deeplynx_hook.get_data_sources_api()

            # Create manual import
            response = data_sources_api.create_manual_import_from_path(self.container_id, self.data_source_id, import_path=self.file_path)
            # pprint(response)
            print(type(response.get('isError')))
            print(response.get('isError'))

            # Check if the response indicates success
            if response.get('isError') != False:
                raise AirflowException(f"Failed to create manual import: {response}")

        except Exception as e:
            # Raise an AirflowException with the error message
            raise AirflowException(f"An error occurred while creating the manual import: {str(e)}")
