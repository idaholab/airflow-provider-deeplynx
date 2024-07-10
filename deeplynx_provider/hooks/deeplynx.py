from airflow.hooks.base import BaseHook
from deep_lynx.configuration import Configuration
import deep_lynx
import json
from typing import Any, Dict

class DeepLynxHook(BaseHook):
    conn_name_attr = 'deeplynx_conn_id'
    default_conn_name = 'deeplynx_default'
    conn_type = 'deeplynx'
    hook_name = 'DeepLynx'

    def __init__(self, deeplynx_config: Configuration, token: str = None):
        super().__init__()
        self.deeplynx_config = deeplynx_config
        self.token = token

    def get_deep_lynx_client(self, bearerAuth=True):
        """Create a new deep_lynx client and set authorization token"""
        deep_lynx_client = deep_lynx.ApiClient(self.deeplynx_config)
        if bearerAuth == True:
            deep_lynx_client.set_default_header('Authorization', f'Bearer {self.token}')

        return deep_lynx_client

    ###########################################
    ##### get deeplynx apis
    def get_data_query_api(self):
        from deep_lynx.api.data_query_api import DataQueryApi
        deep_lynx_client = self.get_deep_lynx_client()
        return DataQueryApi(deep_lynx_client)

    def get_time_series_api(self):
        # from deep_lynx.api.time_series_api import TimeSeriesApi
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.TimeSeriesApi(deep_lynx_client)

    def get_data_sources_api(self):
        from deep_lynx.api.data_sources_api import DataSourcesApi
        deep_lynx_client = self.get_deep_lynx_client()
        return DataSourcesApi(deep_lynx_client)

    def get_authentication_api(self):
        deep_lynx_client = self.get_deep_lynx_client()
        return deep_lynx.AuthenticationApi(deep_lynx_client)

    ###########################################
    ##### deeplynx connection widget and field
    # @staticmethod
    # def get_connection_form_widgets() -> Dict[str, Any]:
    #     """Returns connection widgets to add to connection form"""
    #     from flask_appbuilder.fieldwidgets import BS3PasswordFieldWidget, BS3TextFieldWidget
    #     from flask_babel import lazy_gettext
    #     from wtforms import PasswordField, StringField
    #     from wtforms.validators import DataRequired
    #
    #     return {
    #         "host": StringField(lazy_gettext('DeepLynx URL'), widget=BS3TextFieldWidget(), validators=[DataRequired()]),
    #         "login": StringField(lazy_gettext('API Key'), widget=BS3TextFieldWidget(), validators=[DataRequired()]),
    #         "password": PasswordField(lazy_gettext('API Secret'), widget=BS3PasswordFieldWidget(), validators=[DataRequired()]),
    #     }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        return {
            # "hidden_fields": ['host', 'schema', 'login', 'password', 'port', 'extra'],
            "hidden_fields": ['schema', 'port', 'extra'],
            "relabeling": {
                "host": "DeepLynx URL",
                "login": "API Key",
                "password": "API Secret",
            },
        }
