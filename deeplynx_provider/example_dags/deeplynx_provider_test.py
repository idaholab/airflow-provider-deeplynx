from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.create_container_operator import CreateContainerOperator
from deeplynx_provider.operators.import_container_operator import ImportContainerOperator
from deeplynx_provider.operators.set_data_source_active_operator import SetDataSourceActiveOperator
from deeplynx_provider.operators.create_manual_import_from_path_operator import CreateManualImportFromPathOperator
from deeplynx_provider.operators.timeseries_query_operator import TimeSeriesQueryOperator
from deeplynx_provider.operators.timeseries_query_all_operator import TimeSeriesQueryAllOperator
from deeplynx_provider.operators.upload_file_operator import UploadFileOperator
import os

# get local data paths
dag_directory = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(dag_directory, 'data')
container_export_name = "Container_export.json"
container_export_path = os.path.join(data_dir, container_export_name)
timeseries_data_name = "tc_201.csv"
import_data_path = os.path.join(data_dir, timeseries_data_name)

default_args = {
    'owner': 'jack',
    'concurrency': 1,
    'retries': 0,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag_params = {
  "connection_id": "",
  "container_name": "My DeepLynx Airflow Provider Test",
  "data_source_name": "TC-201",
}

dag = DAG(
    'deeplynx_provider_test',
    default_args=default_args,
    description='self contained functional test',
    schedule=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ params.connection_id }}',
    dag=dag
)

create_container = CreateContainerOperator(
    task_id='create_container',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_name='{{ params.container_name }}',
    container_description = "testing the airflow deeplynx_provider",
    dag=dag
)

import_container = ImportContainerOperator(
    task_id='import_container',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    file_path = container_export_path,
    import_ontology = True,
    import_data_sources = True,
    # import_type_mappings = True,
    dag=dag
)

set_data_source_active = SetDataSourceActiveOperator(
    task_id='set_data_source_active',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_name = '{{ params.data_source_name }}',
    timeseries = True,
    dag=dag
)

import_timeseries_data = CreateManualImportFromPathOperator(
    task_id='import_timeseries_data',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id = "{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    file_path = import_data_path,
    dag=dag
)

query_timeseries = TimeSeriesQueryOperator(
    task_id='query_timeseries',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    properties=[
    "timestamp",
    "seconds",
    "tc_201"],
    query_params={'limit': 1000, 'sort_by': 'timestamp', 'sort_desc': True},
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id = "{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    write_to_file=True,
    dag=dag
)

query_timeseries_all = TimeSeriesQueryAllOperator(
    task_id='timeseries_query_all',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    dag=dag
)

upload_result = UploadFileOperator(
    task_id='upload_result',
    conn_id='{{ params.connection_id }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id = "{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    file_path = "{{ ti.xcom_pull(task_ids='query_timeseries', key='file_path') }}",
    dag=dag
)


get_token >> create_container >> import_container >> set_data_source_active >> import_timeseries_data >> [query_timeseries, query_timeseries_all] >> upload_result
