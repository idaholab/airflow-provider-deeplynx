from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.models.create_container_request import CreateContainerRequest
from deep_lynx.models.list_container_response import ListContainerResponse  # Assuming this is where ListContainerResponse is defined
from deep_lynx.configuration import Configuration

class ImportContainerOperator(DeepLynxBaseOperator):
    """
    ImportContainerOperator is an Airflow operator to import another container's exported ontology, data sources, and type mappings.

    This operator requires a connection ID and the name of the container to check or create.

    Attributes:
        container_id (str): The name of the container to check or create.
        file_path (str): The local path to the exported container that you want to import.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'file_path')

    @apply_defaults
    def __init__(self, container_id: str, file_path: str, dryrun: bool = False, import_ontology: bool = False, import_data_sources: bool = False, import_type_mappings: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize ImportContainerOperator with the given parameters.

        Args:
            container_id (str): The name of the container to check or create.
            file_path (str): The local path to the exported container that you want to import.
            dryrun (bool, optional): Whether to perform a dry run of the import.
            import_ontology (bool, optional): Whether to import the ontology.
            import_data_sources (bool, optional): Whether to import the data sources.
            import_type_mappings (bool, optional): Whether to import the type mappings.
            conn_id (str, optional): The connection ID to use.
            host (str, optional): The host for the DeepLynx API.
            deeplynx_config (dict, optional): Additional configuration for DeepLynx.
            token (str, optional): The token for authentication.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.file_path = file_path
        self.dryrun = dryrun
        self.import_ontology = import_ontology
        self.import_data_sources = import_data_sources
        self.import_type_mappings = import_type_mappings

        # Ensure import_type_mappings can only be true if import_ontology and import_data_sources are both true
        if self.import_type_mappings and not (self.import_ontology and self.import_data_sources):
            raise ValueError("import_type_mappings can only be true if import_ontology and import_data_sources are both true")

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        containers_api = deeplynx_hook.get_containers_api()

        # import container
        response = containers_api.import_container(
            export_file=self.file_path,
            container_id=self.container_id,
            dryrun=self.dryrun,
            import_ontology=self.import_ontology,
            import_data_sources=self.import_data_sources,
            import_type_mappings=self.import_type_mappings
        )

        # convert ContainerImportResponse to dict
        response_dict = response.to_dict()
        # Check success
        if response.is_error is None:
            raise AirflowException(f"Failed to import container: {response_dict}")
        elif response.is_error == True:
            raise AirflowException(f"Failed to import container: {response_dict}")

        # Push to xcom
        task_instance = context['task_instance']
        task_instance.xcom_push(key='import_container_response', value=response_dict)
