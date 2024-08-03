# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow.hooks.base import BaseHook
from deep_lynx.configuration import Configuration
import deep_lynx
import json
from typing import Any, Dict

class DeepLynxHook(BaseHook):
    """
    DeepLynxHook is an Airflow hook to interact with the DeepLynx API.

    This hook initializes the DeepLynx client with the provided configuration and token,
    and provides methods to access various DeepLynx APIs.

    Attributes:
        conn_name_attr (str): The name attribute for the DeepLynx connection.
        default_conn_name (str): The default connection name for DeepLynx.
        conn_type (str): The connection type for DeepLynx.
        hook_name (str): The name of the hook.
        deeplynx_config (Configuration): The configuration object for DeepLynx.
        token (str, optional): The token for authentication.
    """

    conn_name_attr = 'deeplynx_conn_id'
    default_conn_name = 'deeplynx_default'
    conn_type = 'deeplynx'
    hook_name = 'DeepLynx'

    def __init__(self, deeplynx_config: Configuration, token: str = None):
        """
        Initialize DeepLynxHook with the given configuration and token.

        Args:
            deeplynx_config (Configuration): The configuration object for DeepLynx.
            token (str, optional): The token for authentication.
        """
        super().__init__()
        self.deeplynx_config = deeplynx_config
        self.token = token

    def get_deep_lynx_client(self, bearerAuth=True):
        """
        Create a new DeepLynx client and set the authorization token.

        Args:
            bearerAuth (bool, optional): Whether to use Bearer authentication. Defaults to True.

        Returns:
            deep_lynx.ApiClient: The initialized DeepLynx API client.
        """
        deep_lynx_client = deep_lynx.ApiClient(self.deeplynx_config)
        if bearerAuth:
            deep_lynx_client.set_default_header('Authorization', f'Bearer {self.token}')

        return deep_lynx_client

    ###########################################
    ##### Get DeepLynx APIs
    def get_data_query_api(self):
        """
        Get the DataQuery API client.

        Returns:
            deep_lynx.DataQueryApi: The DataQuery API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.DataQueryApi(deep_lynx_client)

    def get_time_series_api(self):
        """
        Get the TimeSeries API client.

        Returns:
            deep_lynx.TimeSeriesApi: The TimeSeries API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.TimeSeriesApi(deep_lynx_client)

    def get_data_sources_api(self):
        """
        Get the DataSources API client.

        Returns:
            deep_lynx.DataSourcesApi: The DataSources API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.DataSourcesApi(deep_lynx_client)

    def get_authentication_api(self):
        """
        Get the Authentication API client.

        Returns:
            deep_lynx.AuthenticationApi: The Authentication API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.AuthenticationApi(deep_lynx_client)

    def get_containers_api(self):
        """
        Get the Containers API client.

        Returns:
            deep_lynx.ContainersApi: The Containers API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.ContainersApi(deep_lynx_client)
    
    def get_graph_api(self):
        """
        Get the Graph API client.

        Returns:
            deep_lynx.GraphApi: The Graph API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.GraphApi(deep_lynx_client)

    def get_type_mapping_api(self):
        """
        Get the DataTypeMappings API client.

        Returns:
            deep_lynx.DataTypeMappingsApi: The DataTypeMappings API client.
        """
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.DataTypeMappingsApi(deep_lynx_client)

    ###########################################
    ##### DeepLynx Connection Field
    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """
        Returns custom field behaviour for the connection form.

        This method customizes the visibility and labels of the fields in the connection form.

        Returns:
            Dict: The custom field behaviour configuration.
        """
        return {
            "hidden_fields": ['schema', 'port', 'extra'],
            "relabeling": {
                "host": "DeepLynx URL",
                "login": "API Key",
                "password": "API Secret",
            },
        }
