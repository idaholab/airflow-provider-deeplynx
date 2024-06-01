from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from deeplynx_provider.operators.utils import reconstruct_config_str
import deep_lynx
from deep_lynx.configuration import Configuration

class GetOauthTokenOperator(BaseOperator):
    template_fields = ('conn_id', 'host', 'api_key', 'api_secret', 'deeplynx_config')

    @apply_defaults
    def __init__(self, conn_id=None, host=None, api_key=None, api_secret=None, deeplynx_config: dict = None, expiry='1hr', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.host = host
        self.api_key = api_key
        self.api_secret = api_secret
        self.deeplynx_config = deeplynx_config
        self.expiry = expiry

    def execute(self, context):
        """
        Execution logic:
        1. If conn_id is provided:
           - Fetch API key, secret, and optionally host from the Airflow connection.
           - Create a default deeplynx_config with verify_ssl set to False.
        2. If deeplynx_config is provided:
           - Use the provided deeplynx_config object.
           - Ensure API key and secret are provided.
        3. If neither deeplynx_config nor conn_id is provided:
           - Ensure host, API key, and secret are provided.
           - Create a default deeplynx_config with verify_ssl set to False.
        4. Authenticate using the deeplynx_config, API key, and secret.
        5. Push the token and host to XCom.
        """
        if self.conn_id:
            # If conn_id is provided, use it to set api_key, api_secret, and optionally host
            conn = BaseHook.get_connection(self.conn_id)
            if not all([conn.login, conn.password]):
                raise AirflowException("Missing API key and secret in the Airflow connection.")
            self.api_key = conn.login
            self.api_secret = conn.password
            if not self.host:
                self.host = conn.host
        else:
            # api_key and api_secret needs to come from either conn_id or operator params, check for that here
            if not all([self.api_key, self.api_secret]):
                raise AirflowException("conn_id OR api_key/api_secret must be provided.")

        if self.deeplynx_config:
            # Use provided deeplynx_config
            config = reconstruct_config_str(self.deeplynx_config)
        else:
            # Ensure host is provided if not using deeplynx_config
            if not self.host:
                raise AirflowException("Host must be provided if not using a custom deeplynx_config.")
            config = self.create_default_config()

        token = self.authenticate(config, self.api_key, self.api_secret, self.expiry)
        # Push the token and host to XCom
        context['task_instance'].xcom_push(key='token', value=token)
        context['task_instance'].xcom_push(key='host', value=self.host)

    def create_default_config(self):
        deeplynx_config = deep_lynx.Configuration()
        deeplynx_config.host = self.host
        deeplynx_config.verify_ssl = False
        # deeplynx_config.debug = True
        return deeplynx_config

    def authenticate(self, deeplynx_config, api_key, api_secret, x_api_expiry):
        """Authenticate and return a new token."""
        if not all([api_key, api_secret]):
            raise ValueError("API key and secret are required to authenticate.")

        client = deep_lynx.ApiClient(deeplynx_config)
        auth_api = deep_lynx.AuthenticationApi(client)
        token = auth_api.retrieve_o_auth_token(x_api_key=api_key, x_api_secret=api_secret, x_api_expiry=x_api_expiry)
        return token
