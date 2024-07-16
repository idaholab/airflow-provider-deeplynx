from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.models.create_container_request import CreateContainerRequest
from deep_lynx.models.list_container_response import ListContainerResponse  # Assuming this is where ListContainerResponse is defined
from deep_lynx.configuration import Configuration

class CreateContainerOperator(DeepLynxBaseOperator):
    """
    Check if a container with the given name exists. If it exists, the operator retrieves the container ID.
    Otherwise, it creates a new container and returns the new container ID.

    This operator requires a connection ID and the name of the container to check or create.

    Attributes:
        container_name (str): The name of the container to check or create.
        container_description (str, optional): The description of the container to create.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_name', 'container_description')

    @apply_defaults
    def __init__(self, container_name: str, container_description: str = None, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize CreateContainerOperator with the given parameters.

        Args:
            container_name (str): The name of the container to check or create.
            container_description (str, optional): The description of the container to create.
            conn_id (str, optional): The connection ID to use.
            host (str, optional): The host for the DeepLynx API.
            deeplynx_config (dict, optional): Additional configuration for DeepLynx.
            token (str, optional): The token for authentication.
            *args: Variable length argument list.
            **kwargs: Arbitrary keyword arguments.
        """
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_name = container_name
        self.container_description = container_description

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method checks if the container with the specified name exists.
        If it exists, it retrieves the container ID. Otherwise, it creates a new container
        and retrieves the new container ID.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        containers_api = deeplynx_hook.get_containers_api()

        # List containers
        containers_response = containers_api.list_containers()

        # Ensure the response is of type ListContainerResponse
        if not isinstance(containers_response, ListContainerResponse):
            raise ValueError("Expected ListContainerResponse, got {}".format(type(containers_response)))

        # Check if container exists
        container_id = None
        for container in containers_response.value:
            if container.name == self.container_name:
                container_id = container.id
                break

        if container_id is None:
            # Container not found, create a new one
            new_container = containers_api.create_container(CreateContainerRequest(
                self.container_name, self.container_description, False
            ))
            container_id = new_container.value[0].id

        # Push containerId to xcom
        task_instance = context['task_instance']
        task_instance.xcom_push(key='container_id', value=container_id)
