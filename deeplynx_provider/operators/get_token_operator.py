from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class GetTokenOperator(BaseOperator):
    # By adding template_fields, the specified fields will be automatically templated by Airflow when the DAG is executed. This means that any template variables used in these fields, like {{ params.CONTAINER_ID }}, will be appropriately replaced with their corresponding values from the DAG's params or other sources like XCom.
    template_fields = ('conn_id', 'api_key', 'api_secret', 'expiry')

    @apply_defaults
    def __init__(self, conn_id, api_key, api_secret, expiry, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.api_key = api_key
        self.api_secret = api_secret
        self.expiry = expiry

    def execute(self, context):
        import os
        import json
        import deep_lynx
        from deeplynx_provider.hooks.deeplynx import DeepLynxHook

        ### create an instance of the API class
        hook = DeepLynxHook(self.conn_id)
        token = hook._authenticate(self.api_key, self.api_secret, self.expiry)
        # Push the token to XCom
        context['task_instance'].xcom_push(key='token', value=token)
