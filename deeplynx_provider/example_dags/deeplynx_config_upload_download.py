from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.configuration_operator import DeepLynxConfigurationOperator
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.upload_file_operator import UploadFileOperator
from deeplynx_provider.operators.download_file_operator import DownloadFileOperator
import os

# get local data paths
dag_directory = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(dag_directory, 'data')
import_data_name = "piping_tutorial.mp4"
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
  "download_file_directory": "/usr/local/airflow/logs/custom_download_directory",
}

dag = DAG(
    'deeplynx_config_upload_download',
    default_args=default_args,
    description='',
    schedule=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

create_config = DeepLynxConfigurationOperator(
    task_id='create_config',
    conn_id='{{ params.connection_id }}',
    temp_folder_path='{{ params.download_file_directory }}',
    dag=dag
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ params.connection_id }}',
    dag=dag
)

upload_file = UploadFileOperator(
    task_id='upload_file',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id='{{ params.container_id }}',
    data_source_id = '{{ params.data_source_id }}',
    file_path = import_data_path,
    dag=dag
)

download_file = DownloadFileOperator(
    task_id='download_file',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id='{{ params.container_id }}',
    file_id="{{ ti.xcom_pull(task_ids='upload_file', key='file_id') }}",
    dag=dag
)


create_config >> get_token >> upload_file >> download_file
