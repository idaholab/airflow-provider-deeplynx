from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.timeseries_query_operator import TimeSeriesQueryOperator

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

# these will get sent in rest post to trigger the dag; is this secure?
dag_params = {
  "host": "https://deeplynx.azuredev.inl.gov",
  "container_id": "",
  "data_source_id": "",
  "api_key": "",
  "api_secret": "",
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
    host='{{ params.host }}',
    api_key='{{ params.api_key }}',
    api_secret='{{ params.api_secret }}',
    dag=dag
)

query_timeseries_1 = TimeSeriesQueryOperator(
    task_id='timeseries_query_1',
    host='{{ params.host }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    properties=[
    "timestamp",
    "seconds",
    "tc_te_1_1",
    "tc_te_1_2",
    "tc_te_1_3",
    "tc_te_1_4"],
    query_params={'limit': 5, 'sort_by': 'timestamp', 'sort_desc': True},
    container_id='{{ params.container_id }}',
    data_source_id='{{ params.data_source_id }}',
    write_to_file=False,
    dag=dag
)

query_timeseries_2 = TimeSeriesQueryOperator(
    task_id='timeseries_query_2',
    host='{{ params.host }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    properties=[
    "timestamp",
    "seconds",
    "tc_te_3_1",
    "tc_te_3_2",
    "tc_te_3_3"],
    query_params={'limit': 5, 'sort_by': 'timestamp', 'sort_desc': True},
    container_id='{{ params.container_id }}',
    data_source_id='{{ params.data_source_id }}',
    dag=dag
)

get_token_task >> query_timeseries_1 >> query_timeseries_2
