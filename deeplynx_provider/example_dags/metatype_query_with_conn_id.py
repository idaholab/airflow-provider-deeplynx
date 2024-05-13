from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.metatype_query_operator import MetatypeQueryOperator
from airflow.configuration import conf

#####################################
log_folder = conf.get('logging', 'base_log_folder')
data_folder = f"{log_folder}/data"
#####################################

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
  "connection_id": "testing_provider_package",
  "container_id": "5",
  "metatype_name": "Entity",
  "data_folder": data_folder
}

dag = DAG(
    'metatype_query_with_conn_id',
    default_args=default_args,
    description='DAG for querying timeseries data from DeepLynx',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    conn_id=dag.params['connection_id'],
    dag=dag
)

query_metatype1 = MetatypeQueryOperator(
    task_id='query_metatype_write_to_file',
    host="{{ ti.xcom_pull(task_ids='get_token', key='host') }}",
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    metatype_name='{{ params.metatype_name }}',
    container_id='{{ params.container_id }}',
    file_path='{{ params.data_folder }}/{{ run_id }}_{{ task.task_id }}.json',
    dag=dag
)

query_metatype2 = MetatypeQueryOperator(
    task_id='query_metatype_pass_to_xcom',
    host="{{ ti.xcom_pull(task_ids='get_token', key='host') }}",
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    metatype_name='{{ params.metatype_name }}',
    container_id='{{ params.container_id }}',
    write_to_file=False,
    dag=dag
)


get_token_task >> query_metatype1 >> query_metatype2
