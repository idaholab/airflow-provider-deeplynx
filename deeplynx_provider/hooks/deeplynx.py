from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
import deep_lynx
from deep_lynx.configuration import Configuration
import os


class DeepLynxHook(BaseHook):
    def __init__(self, deeplynx_config: Configuration, token: str):
        super().__init__()
        self.deeplynx_config = deeplynx_config
        self.token = token

    def get_deep_lynx(self, deeplynx_config):
        """Create a new deep_lynx client and set authorization token"""
        deep_lynx_client = deep_lynx.ApiClient(deeplynx_config)
        deep_lynx_client.set_default_header('Authorization', f'Bearer {self.token}')

        return deep_lynx_client

    #### get specific deep_lynx.api's
    def get_data_query_api(self):
        from deep_lynx.api.data_query_api import DataQueryApi
        ### create an instance of the API class
        deep_lynx_client = self.get_deep_lynx(self.deeplynx_config)
        data_query = DataQueryApi(deep_lynx_client)

        return data_query

    def get_time_series_api(self):
        from deep_lynx.api.time_series_api import TimeSeriesApi
        ### create an instance of the API class
        deep_lynx_client = self.get_deep_lynx(self.deeplynx_config)
        time_series = TimeSeriesApi(deep_lynx_client)

        return time_series

    def get_data_sources_api(self):
        from deep_lynx.api.data_sources_api import DataSourcesApi

        print("self.deeplynx_config:")
        print(self.deeplynx_config.host)
        print(self.deeplynx_config.verify_ssl)
        print(self.deeplynx_config.temp_folder_path)

        ### create an instance of the API class
        deep_lynx_client = self.get_deep_lynx(self.deeplynx_config)
        data_sources = DataSourcesApi(deep_lynx_client)

        return data_sources
