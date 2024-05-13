from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
  "host": "http://host.docker.internal:8090",
  "key": "YjhkZjQ1ZTAtZmMwYS00Yzk1LTg2OTAtMDgwZjAyMTRjZjlk",
  "secret": "NTdkMTEwOTItNTMwZC00YjJmLWI2YjYtZjQ5OGYxM2U0OTg5",
}

dag = DAG(
    'get_token_with_key_secret',
    default_args=default_args,
    description='',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    host=dag.params['host'],
    api_key=dag.params['key'],
    api_secret=dag.params['secret'],
    dag=dag
)

get_token_task
