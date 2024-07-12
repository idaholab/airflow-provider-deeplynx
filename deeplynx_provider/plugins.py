from airflow.plugins_manager import AirflowPlugin
import os

class ExampleDagsPlugin(AirflowPlugin):
    name = 'example_dags_plugin'
    def __init__(self):
        example_dags_path = os.path.join(os.path.dirname(__file__), 'example_dags')
        if os.path.exists(example_dags_path):
            dags_folder = os.environ.get('AIRFLOW__CORE__DAGS_FOLDER')
            if dags_folder:
                os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = f'{dags_folder},{example_dags_path}'
            else:
                os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = example_dags_path

plugin_instance = ExampleDagsPlugin()
