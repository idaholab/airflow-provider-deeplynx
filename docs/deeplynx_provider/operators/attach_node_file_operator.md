Module deeplynx_provider.operators.attach_node_file_operator
============================================================






Classes
-------

`AttachFileOperator(container_id: str, node: str, file_id: str, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   DeepLynxBaseOperator is the base for the majority of the operators in deeplynx_provider.
    Used to perform common setup and configuration for interacting with the DeepLynx API. Subclasses must
    implement the `do_custom_logic` method to define the specific logic for the operator.
    
    Attributes:
        token (str, optional): The token for authentication.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
    
    Initialize AttachFileOperator with the given parameters.
    
    Args:
        container_id (str): The ID of the container in DeepLynx.
        node (str): The ID of the node in DeepLynx.
        file_id (str): The ID of the file to attach.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.

    ### Ancestors (in MRO)

    * deeplynx_provider.operators.deeplynx_base_operator.DeepLynxBaseOperator
    * airflow.models.baseoperator.BaseOperator
    * airflow.models.abstractoperator.AbstractOperator
    * airflow.template.templater.Templater
    * airflow.utils.log.logging_mixin.LoggingMixin
    * airflow.models.taskmixin.DAGNode
    * airflow.models.taskmixin.DependencyMixin

    ### Class variables

    `template_fields`
    :