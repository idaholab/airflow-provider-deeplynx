from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.timeseries_query_all_operator import TimeSeriesQueryAllOperator
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
  "connection_id": "",
  "container_id": "",
  "data_source_id": "",
}

dag = DAG(
    'timeseries_query_all',
    default_args=default_args,
    description='DAG for querying timeseries data from DeepLynx',
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

query_timeseries_all = TimeSeriesQueryAllOperator(
    task_id='timeseries_query_all',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id='{{ params.container_id }}',
    data_source_id='{{ params.data_source_id }}',
    dag=dag
)

get_token_task >> query_timeseries_all
