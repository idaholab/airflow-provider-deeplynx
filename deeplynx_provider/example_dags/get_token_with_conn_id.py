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

dag = DAG(
    'get_token_with_conn_id',
    default_args=default_args,
    description='',
    schedule_interval=None,
    catchup=False,
    params={"connection_id": "testing_provider_package"},
    max_active_runs=1
)

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    conn_id=dag.params['connection_id'],
    dag=dag
)

get_token_task
