from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
from airflow.utils.decorators import apply_defaults
from deep_lynx.configuration import Configuration
import deep_lynx

class DeepLynxBaseOperator(BaseOperator):
    template_fields = ('host', 'deep_lynx_config', 'token')

    @apply_defaults
    def __init__(self, host: str = None, deep_lynx_config: Configuration = None, token: str = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if token is None:
            raise AirflowException("Please provide a 'token'.")
        elif host is None and deep_lynx_config is None:
            raise AirflowException("Please provide a host or deep_lynx_config.")
        #
        self.host = host
        self.deep_lynx_config = deep_lynx_config
        self.token = token


    def execute(self, context):
        from deeplynx_provider.hooks.deeplynx import DeepLynxHook

        # if user provides host, use a default configuration with that host
        if self.host:
            configuration = Configuration()
            configuration.host = self.host
            self.deep_lynx_config = configuration

        # TODO: add checks for specific required params in deep_lynx_config here
        # could also add further checks for specific or type of operators; deep_lynx.download_file needs additional config for example
        deeplynx_hook = DeepLynxHook(self.deep_lynx_config, self.token)
        # Common setup logic and call to the method that must be implemented by each subclass
        return self.do_custom_logic(context, deeplynx_hook)

    def do_custom_logic(self, context, deeplynx_hook):
        raise NotImplementedError("Subclasses must implement this method.")

    # many deelynx operators retrieve json data that should either be writen to storage or the data should be passed to xcom directly
    def write_or_push_to_xcom(self, context, data, file_path=None, write_to_file=True):
        import os
        import json

        task_instance = context['task_instance']
        if write_to_file:
            if not file_path:
                raise ValueError("file_path must be provided if write_to_file is True")
            # Ensure the directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            # Write response to a file
            with open(file_path, 'w') as f:
                json.dump(data, f)
            # Push file_path to XCom
            task_instance.xcom_push(key='file_path', value=file_path)
        else:
            # Push data to XCom
            task_instance.xcom_push(key='data', value=data)
