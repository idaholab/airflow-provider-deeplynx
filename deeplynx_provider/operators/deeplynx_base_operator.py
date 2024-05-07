from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DeepLynxBaseOperator(BaseOperator):
    @apply_defaults
    def __init__(self, conn_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id

    def execute(self, context):
        raise NotImplementedError("This method should be overridden by subclasses.")
