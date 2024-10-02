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
    "connection_id": "",
    "container_id": "",
    "root_node_id": "",
    "depth": "8",
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
        conn_id='{{ dag_run.conf["connection_id"] }}',
    )

    # Task to query the graph
    query_graph = DataQueryWithParamsOperator(
        task_id='query_graph_with_params',
        conn_id='{{ dag_run.conf["connection_id"] }}',
        token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
        query_type=QueryType.GRAPH,
        properties={'root_node': '{{ dag_run.conf["root_node_id"] }}', 'depth': '{{ dag_run.conf["root_node_id"] }}'},
        container_id='{{ dag_run.conf["container_id"] }}',
        write_to_file=False,
    )

    @task
    def parse_query(data):
        import json

        json_data = json.loads(data)
        parsed_data = []

        if json_data:
            for object in json_data:
                # print(object)
                # Extract some infos
                origin_id = object["origin_id"]
                if origin_id:
                    parsed_data.append(origin_id)

        return parsed_data

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
