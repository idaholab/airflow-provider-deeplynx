# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow.decorators import task, dag
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.data_query_with_params_operator import DataQueryWithParamsOperator, QueryType

#####################################

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
    "connection_id": "deeplynx.dev",
    "container_id": "318",
}

@dag(
    default_args=default_args,
    description='Demonstrates DeepLynx graph query and data parsing, while using TaskFlow API.',
    schedule_interval=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1,
    start_date=datetime(2024, 1, 1),
)
def data_query_then_parse():
    # Task to get OAuth token
    get_token = GetOauthTokenOperator(
        task_id='get_token',
        conn_id='{{ params.connection_id }}',  # Using params instead of dag_run.conf to avoid runtime templating issues
    )

    # Task to query the graph
    query_graph = DataQueryWithParamsOperator(
        task_id='query_graph_with_params',
        conn_id='{{ params.connection_id }}',
        token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
        query_type=QueryType.GRAPH,
        properties={'root_node': '5128649', 'depth': '8'},
        container_id='{{ params.container_id }}',
        write_to_file=False,
    )

    # Python task to parse the query data
    @task
    def parse_query(data):
        import json

        json_data = json.loads(data)
        print(type(json_data))
        parsed_data = []

        if json_data:
            for object in json_data:
                print(object)
                # Extract some infos
                origin_id = object["origin_id"]
                if origin_id:
                    parsed_data.append(origin_id)

        return parsed_data

    # Python task to print the parsed data (or you can use it for further processing)
    @task
    def process_parsed_data(parsed_data):
        if parsed_data:
            for id in parsed_data:
                print(f"Origin Id: {id}")

    # Set dependencies using returned values
    get_token >> query_graph

    parsed_data = parse_query(query_graph.output["data"])
    process_parsed_data(parsed_data)

# Instantiate the DAG
data_query = data_query_then_parse()
