# Copyright 2024, Battelle Energy Alliance, LLC, All Rights Reserved

from airflow import DAG
from datetime import datetime
from deeplynx_provider.operators.get_token_operator import GetOauthTokenOperator
from deeplynx_provider.operators.create_container_operator import CreateContainerOperator
from deeplynx_provider.operators.import_container_operator import ImportContainerOperator
from deeplynx_provider.operators.set_data_source_active_operator import SetDataSourceActiveOperator
from deeplynx_provider.operators.set_type_mapping_active_operator import SetTypeMappingActiveOperator
from deeplynx_provider.operators.create_manual_import_from_path_operator import CreateManualImportFromPathOperator
import os

# get local data paths
dag_directory = os.path.dirname(os.path.abspath(__file__))
data_dir = os.path.join(dag_directory, 'data')
container_export_name = "MOOSE_Container_Export.json"
container_export_path = os.path.join(data_dir, container_export_name)
input_file_data_name = "moose_input_file_import.json"
import_data_path = os.path.join(data_dir, input_file_data_name)

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
    "data_source_name": "MOOSE input file as graph",
}

dag = DAG(
    'deeplynx_provider_test',
    default_args=default_args,
    description=(
        'A functional test DAG for the `airflow-provider-deeplynx` package. '

    ),
    doc_md=('Users should create a DeepLynx connection in Airflow with `URL`, `API Key`, '
    'and `API Secret`. To run the DAG, supply the DeepLynx `connection_id`, '
    'optionally create a new `container_name`, and keep `data_source_name` as `MOOSE input file as graph`.'
    'This dag will create a new DeepLynx container with an ontology, data source, and typemappings. It will them upload data, which will initiate DeepLynx data graph creation.'
    ),
    schedule=None,
    catchup=False,
    params=dag_params,
    max_active_runs=1
)

get_token = GetOauthTokenOperator(
    task_id='get_token',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    dag=dag
)

create_container = CreateContainerOperator(
    task_id='create_container',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_name=dag.params["container_name"],
    container_description="testing the airflow deeplynx_provider",
    dag=dag
)

import_container = ImportContainerOperator(
    task_id='import_container',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    file_path=container_export_path,
    import_ontology=True,
    import_data_sources=True,
    import_type_mappings = True,
    dag=dag
)

set_data_source_active = SetDataSourceActiveOperator(
    task_id='set_data_source_active',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_name=dag.params["data_source_name"],
    timeseries=False,
    dag=dag
)

set_MOOSEInputFile_active = SetTypeMappingActiveOperator(
    task_id='set_MOOSEInputFile_active',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    match_keys=['id', 'name', 'description', 'MOOSEInputFile'],
    dag=dag
)

set_NamedValue_active = SetTypeMappingActiveOperator(
    task_id='set_NamedValue_active',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    match_keys=['id', 'name', 'value', 'NamedValue', 'parentId'],
    dag=dag
)

set_Block_active = SetTypeMappingActiveOperator(
    task_id='set_Block_active',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    match_keys=['id', 'name', 'type', 'Block', 'parentId'],
    dag=dag
)

set_SubBlock_active = SetTypeMappingActiveOperator(
    task_id='set_SubBlock_active',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    match_keys=['id', 'name', 'type', 'SubBlock', 'parentId'],
    dag=dag
)

set_Parameter_active = SetTypeMappingActiveOperator(
    task_id='set_Parameter_active',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    match_keys=['id', 'name', 'value', 'Parameter', 'parentId'],
    dag=dag
)

import_data = CreateManualImportFromPathOperator(
    task_id='import_data',
    conn_id='{{ dag_run.conf["connection_id"] }}',
    token="{{ ti.xcom_pull(task_ids='get_token', key='token') }}",
    container_id="{{ ti.xcom_pull(task_ids='create_container', key='container_id') }}",
    data_source_id="{{ ti.xcom_pull(task_ids='set_data_source_active', key='data_id') }}",
    file_path=import_data_path,
    dag=dag
)

get_token >> create_container >> import_container >> set_data_source_active >> [set_MOOSEInputFile_active, set_NamedValue_active, set_Block_active, set_SubBlock_active, set_Parameter_active] >> import_data
