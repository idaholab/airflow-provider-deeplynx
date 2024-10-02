from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.upload_file_operator import UploadFileOperator
import os

# Get local data paths
dag_directory = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(dag_directory, 'data')
import_data_name = "lynx_blue.png"
import_data_path = os.path.join(data_dir, import_data_name)

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
    "connection_id": "",
    "container_id": "",
    "data_source_id": "",
}

with DAG(
    'upload_files',
    default_args=default_args,
    description='Upload multiple files in parallel; demonstrates basic dynamic task generation.',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
) as dag:

    # Task to get the token
    get_token_task = GetOauthTokenOperator(
        task_id='get_token',
        conn_id='{{ dag_run.conf["connection_id"] }}'
    )

    # Example list of file paths to upload
    file_paths = [
        import_data_path,
        import_data_path,
    ]

    # Generate upload tasks dynamically using a loop
    for i, file_path in enumerate(file_paths):
        upload_task = UploadFileOperator(
            task_id=f'upload_file_{i}',
            conn_id="deeplynx.dev",
            container_id='{{ dag_run.conf["container_id"] }}',
            data_source_id='{{ dag_run.conf["data_source_id"] }}',
            file_path=file_path,
            token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}"
        )

        # Set dependency on get_token_task
        get_token_task >> upload_task
