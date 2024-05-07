from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.hooks.base_hook import BaseHook
import deep_lynx

class DeepLynxHook(BaseHook):
    def __init__(self, conn_id, sqlite_conn_id='workflow_instances_db'):
        super().__init__()
        self.conn_id = conn_id
        self.sqlite_conn_id = sqlite_conn_id
        self.connection = BaseHook.get_connection(sqlite_conn_id)

    def get_client(self, token=None):
        """Create a new client and set authorization token if provided."""
        conn = self.get_connection(self.conn_id)
        configuration = deep_lynx.Configuration()
        configuration.host = conn.host
        api_client = deep_lynx.ApiClient(configuration)

        if token:
            api_client.set_default_header('Authorization', f'Bearer {token}')

        return api_client

    def _authenticate(self, api_key, api_secret, x_api_expiry):
        """Authenticate and return a new token."""
        client = self.get_client()
        auth_api = deep_lynx.AuthenticationApi(client)
        token = auth_api.retrieve_o_auth_token(x_api_key=api_key, x_api_secret=api_secret, x_api_expiry=x_api_expiry)
        return token
