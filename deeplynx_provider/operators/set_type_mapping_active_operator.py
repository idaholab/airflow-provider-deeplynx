# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
from deeplynx_provider.operators.utils import type_mapping_from_dict, extract_first_matching_nested_object
import logging

class SetTypeMappingActiveOperator(DeepLynxBaseOperator):
    """
    Set a type mapping as active in DeepLynx.

    This is meant to be used with a type mapping that already has transformations made and needs to be set active.
    This is convenient if you're importing type mappings and transformations into a container.

    This operator requires the container ID, data source ID, and match keys to find the
    appropriate type mapping and set it as active. It only looks at the keys and not the values' data types.
    If your data source happens to have two or more type mappings with the exact same set of keys, but different values' data types, this operator might not work for you.
    If we want to support automated type mapping in Airflow, we need to develop a more complete solution.

    Attributes:
        container_id (str): The ID of the container.
        data_source_id (str): The ID of the data source.
        match_keys (list of str): A list of keys to match in the sample payload of type mappings.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    """

    # Extend DeepLynxBaseOperator.template_fields
    template_fields = DeepLynxBaseOperator.template_fields + ('container_id', 'data_source_id', 'match_keys')

    @apply_defaults
    def __init__(self, container_id: str, data_source_id: str, match_keys: list[str], conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs):
        """
        Initialize SetTypeMappingActiveOperator with the given parameters.

        Args:
            container_id (str): The ID of the container.
            data_source_id (str): The ID of the data source.
            match_keys (list of str): A list of keys to match in the sample payload of type mappings.
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
        self.match_keys = match_keys

    def do_custom_logic(self, context, deeplynx_hook):
        """
        Execute the custom logic for the operator.

        This method retrieves type mappings for the specified data source, finds the first
        type mapping with a sample payload matching the provided keys, and sets it as active.

        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.

        Returns:
            None
        """
        # Get API client
        type_mapping_api = deeplynx_hook.get_type_mapping_api()
        try:
            # Get the list of type mappings for a given data source
            mappings_list = type_mapping_api.list_data_type_mappings(self.container_id, self.data_source_id)
            if not mappings_list.is_error:
                nested_key = "sample_payload"
                matched_mapping_dict = extract_first_matching_nested_object(mappings_list.to_dict()["value"], nested_key, self.match_keys)
                if matched_mapping_dict:
                    matched_mapping = type_mapping_from_dict(matched_mapping_dict)
                    matched_mapping.active = True
                    update_mapping = type_mapping_api.update_data_type_mapping(self.container_id, self.data_source_id, matched_mapping.id, body=matched_mapping)
                    if update_mapping.is_error == False:
                        # Push matched_mapping.id to XCom
                        task_instance = context['task_instance']
                        task_instance.xcom_push(key='mapping_id', value=matched_mapping.id)
                    else:
                        raise AirflowException(f"An error occurred in update_data_type_mapping: {update_mapping.value}")
                else:
                    raise AirflowException("No matching type mapping found.")
            else:
                raise AirflowException(f"An error occurred in list_data_type_mappings: {mappings_list.value}")

        except Exception as e:
            logging.error(f"An error occurred: {e}", exc_info=True)
            raise AirflowException(f"An error occurred while setting type mapping active: {e}")
