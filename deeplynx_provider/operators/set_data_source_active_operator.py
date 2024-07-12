from airflow.utils.decorators import apply_defaults
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deep_lynx.configuration import Configuration
from airflow.exceptions import AirflowException
from pprint import pprint

class SetDataSourceActiveOperator(DeepLynxBaseOperator):
    """
    Operator to set a data source active based on the data_source_id. If data_source_name is supplied,
    will search for a data source with the same name and set active if found.

    Attributes:
        container_id (str): ID of the container in DeepLynx.
        data_source_id (str, optional): ID of the data source in DeepLynx. Defaults to None.
        data_source_name (str, optional): Name of the data source in DeepLynx. Defaults to None.
        timeseries (bool, optional): Indicates if the data source is timeseries. Defaults to False.
        conn_id (str, optional): Connection ID for DeepLynx. Defaults to None.
        host (str, optional): Host for DeepLynx. Defaults to None.
        deeplynx_config (dict, optional): Configuration dictionary for DeepLynx. Defaults to None.
        token (str, optional): Token for authentication. Defaults to None.
    """
    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'data_source_name', 'timeseries')

    @apply_defaults
    def __init__(self, container_id: str, data_source_id: str = None, data_source_name: str = None, timeseries: bool = False, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        super().__init__(conn_id=conn_id, host=host, deeplynx_config=deeplynx_config, token=token, *args, **kwargs)
        self.container_id = container_id
        self.data_source_id = data_source_id
        self.data_source_name = data_source_name
        self.timeseries = timeseries

        # Check for both data_source_id and data_source_name being supplied
        if self.data_source_id and self.data_source_name:
            warnings.warn("Both data_source_id and data_source_name are supplied. Using data_source_id and ignoring data_source_name.")
        # Check that at least one of data_source_id or data_source_name is supplied
        if not self.data_source_id and not self.data_source_name:
            raise AirflowException("Either data_source_id or data_source_name must be supplied. Please provide one.")


    def do_custom_logic(self, context, deeplynx_hook):
        """
        Custom logic to set a data source active in DeepLynx.

        Args:
            context (dict): Airflow context dictionary.
            deeplynx_hook (DeepLynxHook): Hook to interact with DeepLynx API.

        Raises:
            AirflowException: If the API call to set the data source active fails.
        """
        try:
            # Get API client
            data_sources_api = deeplynx_hook.get_data_sources_api()

            # If data_source_id is supplied, use that
            if self.data_source_id:
                pass
            # If data_source_name is supplied, use that to get the data_source_id
            elif self.data_source_name:
                list_result = data_sources_api.list_data_sources(self.container_id, timeseries=self.timeseries)
                self.data_source_id = self.find_id_by_name(list_result.to_dict(), self.data_source_name)
                if not self.data_source_id:
                    raise AirflowException(f"Data source with name '{self.data_source_name}' not found.")

            # Set data source active
            active_response = data_sources_api.set_data_source_active(self.container_id, self.data_source_id)
            # pprint(active_response)
            # pprint(type(active_response))

            # Check if the response indicates success and push data_source_id to xcom
            # <class 'deep_lynx.models.generic200_response.Generic200Response'>
            if active_response.is_error == False:
                # Push to xcom
                task_instance = context['task_instance']
                task_instance.xcom_push(key='data_id', value=self.data_source_id)
            else:
                raise AirflowException(f"Failed to set data source active: {active_response}")

        except Exception as e:
            # Raise an AirflowException with the error message
            raise AirflowException(f"An error occurred while setting data source active: {str(e)}")

    def find_id_by_name(self, response, target_name):
        """
        Processes the list_data_sources response to find the id of an object by its name.

        Args:
            response (dict): The response object to be processed (as a dictionary).
            target_name (str): The name to be searched for in the response.

        Returns:
            str: The id of the object with the matching name, or None if not found.

        Raises:
            ValueError: If the response does not meet expected structure.
        """
        if not isinstance(response, dict):
            raise ValueError("Response must be a dictionary.")

        if "is_error" not in response:
            raise ValueError("Response object does not contain 'is_error' property.")

        if response["is_error"]:
            raise ValueError("Response indicates an error.")

        if "value" not in response or not isinstance(response["value"], list):
            raise ValueError("Response object does not contain a valid 'value' property.")

        for item in response["value"]:
            if not isinstance(item, dict):
                continue
            if "name" in item and item["name"] == target_name:
                return item.get("id")

        return None
