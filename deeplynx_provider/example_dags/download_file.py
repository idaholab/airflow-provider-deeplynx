from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.download_file_operator import DownloadFileOperator

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
  "file_id": ""
}

dag = DAG(
    'download_file',
    default_args=default_args,
    description='DownloadFileOperator example',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ params.connection_id }}',
    dag=dag
)

download_file = DownloadFileOperator(
    task_id='download_file',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id='{{ params.container_id }}',
    file_id='{{ params.file_id }}',
    dag=dag
)


get_token_task >> download_file
