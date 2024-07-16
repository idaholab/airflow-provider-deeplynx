# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from airflow.configuration import conf

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
  "deeplynx_url": "https://deeplynx.azuredev.inl.gov",
  "api_key": "",
  "api_secret": "",
}

dag = DAG(
    'get_token_with_key_secret',
    default_args=default_args,
    description='Demonstrates obtaining a DeepLynx token using `GetOauthTokenOperator` by directly specifying `host`, `api_key`, and `api_secret`, without using `conn_id`.'
    schedule=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token_task = GetOauthTokenOperator(
    task_id='get_token',
    host='{{ params.deeplynx_url }}',
    api_key='{{ params.api_key }}',
    api_secret='{{ params.api_secret }}',
    dag=dag
)

get_token_task
