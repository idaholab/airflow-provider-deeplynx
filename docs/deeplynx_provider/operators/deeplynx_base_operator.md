Module deeplynx_provider.operators.deeplynx_base_operator
=========================================================






Classes
-------

`DeepLynxBaseOperator(token: str = None, conn_id: str = None, host: str = None, deeplynx_config: dict = None, *args, **kwargs)`
:   DeepLynxBaseOperator is the base for the majority of the operators in deeplynx_provider.
    Used to perform common setup and configuration for interacting with the DeepLynx API. Subclasses must
    implement the `do_custom_logic` method to define the specific logic for the operator.
    
    Attributes:
        token (str, optional): The token for authentication.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
    
    Initialize DeepLynxBaseOperator with the given parameters.
    
    Args:
        token (str, optional): The token for authentication.
        conn_id (str, optional): The connection ID to use.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

    ### Ancestors (in MRO)

    * airflow.models.baseoperator.BaseOperator
    * airflow.models.abstractoperator.AbstractOperator
    * airflow.template.templater.Templater
    * airflow.utils.log.logging_mixin.LoggingMixin
    * airflow.models.taskmixin.DAGNode
    * airflow.models.taskmixin.DependencyMixin

    ### Descendants

    * deeplynx_provider.operators.create_container_operator.CreateContainerOperator
    * deeplynx_provider.operators.create_manual_import_from_path_operator.CreateManualImportFromPathOperator
    * deeplynx_provider.operators.create_manual_import_operator.CreateManualImportOperator
    * deeplynx_provider.operators.data_query_operator.DataQueryOperator
    * deeplynx_provider.operators.data_query_with_params_operator.DataQueryWithParamsOperator
    * deeplynx_provider.operators.download_file_operator.DownloadFileOperator
    * deeplynx_provider.operators.import_container_operator.ImportContainerOperator
    * deeplynx_provider.operators.metatype_query_operator.MetatypeQueryOperator
    * deeplynx_provider.operators.set_data_source_active_operator.SetDataSourceActiveOperator
    * deeplynx_provider.operators.timeseries_query_all_operator.TimeSeriesQueryAllOperator
    * deeplynx_provider.operators.timeseries_query_operator.TimeSeriesQueryOperator
    * deeplynx_provider.operators.upload_file_operator.UploadFileOperator

    ### Class variables

    `template_fields`
    :

    ### Methods

    `do_custom_logic(self, context, deeplynx_hook)`
    :   Define the custom logic for the operator.
        
        This method must be implemented by subclasses to perform specific tasks.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None

    `execute(self, context)`
    :   Execute the operator's main logic.
        
        This method sets up the DeepLynx configuration, creates a DeepLynxHook,
        and calls the `do_custom_logic` method which must be implemented by subclasses.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
        
        Returns:
            The result of the `do_custom_logic` method.

    `format_query_response_filename(self, context, query_name)`
    :   Format the filename for query response data.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            query_name (str): The name of the query.
        
        Returns:
            str: The formatted filename.

    `get_deeplynx_config(self)`
    :   Retrieve the DeepLynx configuration.
        
        This method constructs the DeepLynx configuration using either `deeplynx_config`,
        `conn_id`, or `host`, and ensures required SSL and temp folder settings are present.
        
        Returns:
            Configuration: The DeepLynx configuration object.

    `save_data(self, data, file_name)`
    :   Save data to a local file.
        
        Args:
            data (str): The data to save.
            file_name (str): The name of the file to save the data to.
        
        Returns:
            str: The path of the saved file.

    `validate_params(self)`
    :   Validate the parameters provided to the operator.
        
        Ensure that only one of `conn_id`, `host`, or `deeplynx_config` is provided,
        and that at least one of them is present. Also ensure that a token is provided.
        
        Raises:
            AirflowException: If the validation fails.

    `write_data(self, file_path, data)`
    :   Write data to a specified file path.
        
        Args:
            file_path (str): The path of the file.
            data (str): The data to write.
        
        Returns:
            None

    `write_data_to_local(self, file_name, data)`
    :   Write data to a local file.
        
        Args:
            file_name (str): The name of the file.
            data (str): The data to write.
        
        Returns:
            str: The path of the saved file.

    `write_or_push_to_xcom(self, context, data, file_name)`
    :   Write data to a file or push it to XCom.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            data (str): The data to write or push.
            file_name (str): The name of the file to write the data to.
        
        Returns:
            None