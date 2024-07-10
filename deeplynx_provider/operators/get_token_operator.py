from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from deeplynx_provider.hooks.deeplynx import DeepLynxHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
from deeplynx_provider.operators.deeplynx_base_operator import DeepLynxBaseOperator
import deep_lynx


class GetOauthTokenOperator(BaseOperator): # different enough to not derive from DeepLynxBaseOperator
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
        # call GetOauthTokenOperator's own validate_params
        self.validate_params()

    def execute(self, context):
        ### process inputs
        config = self.set_class_variable_values()
        ### get hook
        deeplynx_hook = DeepLynxHook(config)
        ### get api client
        auth_api = deeplynx_hook.get_authentication_api()
        ### get token
        token = auth_api.retrieve_o_auth_token(x_api_key=self.api_key, x_api_secret=self.api_secret, x_api_expiry=self.expiry)

        # Push the token and host to XCom
        context['task_instance'].xcom_push(key='token', value=token)
        context['task_instance'].xcom_push(key='host', value=deeplynx_hook.deeplynx_config.host)

    def validate_params(self):
        ## param checks
        # only one of conn_id, or host, or deeplynx_config
        if sum([self.conn_id is not None, self.host is not None, self.deeplynx_config is not None]) >= 2:
            raise AirflowException("Please provide only one of conn_id, host, or deeplynx_config.")
        # at least one of conn_id, or host, or deeplynx_config
        elif self.conn_id is None and self.host is None and self.deeplynx_config is None:
            raise AirflowException("Please provide either a conn_id, a host, or deeplynx_config.")
        # provide either conn_id or api key and secret
        elif self.conn_id is None and (self.api_key is None or self.api_secret is None):
            raise AirflowException("Please provide a DeepLynx conn_id or an api key and secret.")



    def set_class_variable_values(self):
        import json
        from deeplynx_provider.operators.utils import reconstruct_config_str
        """
        input processing logic:
        1. If conn_id is provided:
           - Fetch API key, secret, and optionally host from the Airflow connection.
           - Create a default deeplynx_config with verify_ssl set to False.
        2. If deeplynx_config is provided:
           - Use the provided deeplynx_config object.
           - Ensure API key and secret are provided.
        3. If neither deeplynx_config nor conn_id is provided:
           - Ensure host, API key, and secret are provided.
           - Create a default deeplynx_config with verify_ssl set to False.
         """

        if self.conn_id:
            # If conn_id is provided, use it to set api_key, api_secret, and host
            conn = BaseHook.get_connection(self.conn_id)
            # api_key = json.loads(conn.extra).get('login') if conn.extra else None
            # api_secret = json.loads(conn.extra).get('password') if conn.extra else None
            # host = json.loads(conn.extra).get('host') if conn.extra else None
            api_key = conn.login
            api_secret = conn.password
            host = conn.host
            if not all([api_key, api_secret]):
                raise AirflowException("Missing API key and secret in the Airflow connection.")
            self.api_key = api_key
            self.api_secret = api_secret
            self.host = host
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

        return config


    def create_default_config(self):
        deeplynx_config = deep_lynx.Configuration()
        deeplynx_config.host = self.host
        deeplynx_config.verify_ssl = False
        # deeplynx_config.debug = True
        return deeplynx_config
