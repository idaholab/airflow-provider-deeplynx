from airflow.models import BaseOperator
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException

class GetOauthTokenOperator(BaseOperator):
    # By adding template_fields, the specified fields will be automatically templated by Airflow when the DAG is executed. This means that any template variables used in these fields, like {{ params.CONTAINER_ID }}, will be appropriately replaced with their corresponding values from the DAG's params or other sources like XCom.
    template_fields = ('conn_id', 'host', 'api_key', 'api_secret', 'expiry')

    @apply_defaults
    def __init__(self, conn_id=None, host=None, api_key=None, api_secret=None, expiry='1hr', *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Set conn_id and expiry as an attribute regardless of whether it's provided or not
        self.conn_id = conn_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.expiry = expiry

        # use info in connection id if provided
        if conn_id:
            conn = BaseHook.get_connection(conn_id)
            # first check if provided conn_id have needed config
            if not all([conn.host, conn.login, conn.password]):
                raise AirflowException("Missing deep_lynx_config information in the Airflow connection.")
            else:
                self.host = conn.host
                self.api_key = conn.login
                self.api_secret = conn.password
        else:
            if not all([host, api_key, api_secret]):
                raise AirflowException("Need to provide host, api_key, and api_secret.")
            self.host = host
            self.api_key = api_key
            self.api_secret = api_secret

    def execute(self, context):
        token = self.authenticate(self.host, self.api_key, self.api_secret, self.expiry)
        # Push the token and host to XCom
        context['task_instance'].xcom_push(key='token', value=token)
        context['task_instance'].xcom_push(key='host', value=self.host)

    def authenticate(self, host, api_key, api_secret, x_api_expiry):
        import deep_lynx
        """Authenticate and return a new token."""
        # print(self.host)
        if not all([api_key, api_secret, host]):
            raise ValueError("API key, API secret and host are required to authenticate.")
        configuration = deep_lynx.Configuration()
        configuration.host = host
        client = deep_lynx.ApiClient(configuration)
        auth_api = deep_lynx.AuthenticationApi(client)
        token = auth_api.retrieve_o_auth_token(x_api_key=api_key, x_api_secret=api_secret, x_api_expiry=x_api_expiry)
        return token
