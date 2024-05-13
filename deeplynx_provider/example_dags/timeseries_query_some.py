from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.timeseries_query_operator import TimeSeriesQueryOperator
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

# these will get sent in rest post to trigger the dag; is this secure?
dag_params = {
  "host": "host.docker.internal:8090",
  "container_id": "5",
  "datasource_id": "2",
  "api_key": "YjhkZjQ1ZTAtZmMwYS00Yzk1LTg2OTAtMDgwZjAyMTRjZjlk",
  "api_secret": "NTdkMTEwOTItNTMwZC00YjJmLWI2YjYtZjQ5OGYxM2U0OTg5",
  "data_folder": data_folder,
}

dag = DAG(
    'timeseries_query_some',
    default_args=default_args,
    description='DAG for querying timeseries data from DeepLynx',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    host=dag.params['host'],
    api_key=dag.params['api_key'],
    api_secret=dag.params['api_secret'],
    dag=dag
)

query_timeseries_1 = TimeSeriesQueryOperator(
    task_id='timeseries_query_1',
    host=dag.params['host'],
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    properties=[
    "timestamp",
    "seconds",
    "tc_te_1_1",
    "tc_te_1_2",
    "tc_te_1_3",
    "tc_te_1_4"],
    query_params={'limit': 5, 'sort_by': 'timestamp', 'sort_desc': True},
    container_id=dag.params['container_id'],
    datasource_id=dag.params['datasource_id'],
    write_to_file=False,
    dag=dag
)

query_timeseries_2 = TimeSeriesQueryOperator(
    task_id='timeseries_query_2',
    host=dag.params['host'],
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    properties=[
    "timestamp",
    "seconds",
    "tc_te_3_1",
    "tc_te_3_2",
    "tc_te_3_3"],
    query_params={'limit': 5, 'sort_by': 'timestamp', 'sort_desc': True},
    container_id=dag.params['container_id'],
    datasource_id=dag.params['datasource_id'],
    file_path='{{ params.data_folder }}/{{ run_id }}_{{ task.task_id }}.json',
    dag=dag
)

get_token_task >> query_timeseries_1 >> query_timeseries_2
