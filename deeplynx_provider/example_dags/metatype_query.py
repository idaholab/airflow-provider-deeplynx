from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.configuration_operator import DeepLynxConfigurationOperator
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.metatype_query_operator import MetatypeQueryOperator


#####################################

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
  "metatype_name": "",
}

dag = DAG(
    'metatype_query',
    default_args=default_args,
    description='DAG for querying timeseries data from DeepLynx',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

create_config = DeepLynxConfigurationOperator(
    task_id='create_config',
    conn_id='{{ params.connection_id }}',
    verify_ssl=False,
    dag=dag
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ params.connection_id }}',
    dag=dag
)

query_metatype = MetatypeQueryOperator(
    task_id='query_metatype',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    metatype_name='{{ params.metatype_name }}',
    container_id='{{ params.container_id }}',
    write_to_file=True,
    dag=dag
)


create_config >> get_token >> query_metatype
