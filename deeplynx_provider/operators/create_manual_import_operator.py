from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from airflow.exceptions import AirflowException
from pprint import pprint

class CreateManualImportOperator(DeepLynxBaseOperator):
    """
    Operator to create a manual import in DeepLynx.

    Attributes:
        container_id (str): ID of the container in DeepLynx.
        data_source_id (str): ID of the data source in DeepLynx.
        import_body (dict): Dictionary body for the manual import.
        conn_id (str, optional): Connection ID for DeepLynx. Defaults to None.
        host (str, optional): Host for DeepLynx. Defaults to None.
        deeplynx_config (dict, optional): Configuration dictionary for DeepLynx. Defaults to None.
        token (str, optional): Token for authentication. Defaults to None.
    """
    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'import_body')

    @apply_defaults
    def __init__(self, container_id: str, data_source_id: str, import_body: dict, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.data_source_id = data_source_id
        self.import_body = import_body

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
            response = data_sources_api.create_manual_import(self.container_id, self.data_source_id, body=self.import_body)

            # Check if the response indicates success
            if response.get('isError') != False:
                raise AirflowException(f"Failed to create manual import: {response}")

        except Exception as e:
            # Raise an AirflowException with the error message
            raise AirflowException(f"An error occurred while creating the manual import: {str(e)}")
