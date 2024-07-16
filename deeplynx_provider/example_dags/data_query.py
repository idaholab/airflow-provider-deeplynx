from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.data_query_operator import DataQueryOperator


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
}

dag = DAG(
    'data_query',
    default_args=default_args,
    description='',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ params.connection_id }}',
    dag=dag
)

metatype_query_body = """
{
    metatypes{
        Occurrence {
            name
            NodeId
        }
    }
}
"""

query_metatype = DataQueryOperator(
    task_id='query_metatype',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    query_type="metatype",
    query_body=metatype_query_body,
    container_id='{{ params.container_id }}',
    write_to_file=True,
    dag=dag
)


get_token >> query_metatype
