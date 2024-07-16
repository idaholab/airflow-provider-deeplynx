Module deeplynx_provider.operators.create_container_operator
============================================================






Classes
-------

`CreateContainerOperator(container_name: str, container_description: str = None, conn_id: str = None, host: str = None, deeplynx_config: dict = None, token: str = None, *args, **kwargs)`
:   Check if a container with the given name exists. If it exists, the operator retrieves the container ID.
    Otherwise, it creates a new container and returns the new container ID.
    
    This operator requires a connection ID and the name of the container to check or create.
    
    Attributes:
        container_name (str): The name of the container to check or create.
        container_description (str, optional): The description of the container to create.
        conn_id (str, optional): The connection ID to use for the operation.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
    
    Initialize CreateContainerOperator with the given parameters.
    
    Args:
        container_name (str): The name of the container to check or create.
        container_description (str, optional): The description of the container to create.
        conn_id (str, optional): The connection ID to use.
        host (str, optional): The host for the DeepLynx API.
        deeplynx_config (dict, optional): Additional configuration for DeepLynx.
        token (str, optional): The token for authentication.
        *args: Variable length argument list.
        **kwargs: Arbitrary keyword arguments.

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

    ### Methods

    `do_custom_logic(self, context, deeplynx_hook)`
    :   Execute the custom logic for the operator.
        
        This method checks if the container with the specified name exists.
        If it exists, it retrieves the container ID. Otherwise, it creates a new container
        and retrieves the new container ID.
        
        Args:
            context (dict): The context dictionary provided by Airflow.
            deeplynx_hook (DeepLynxHook): The DeepLynx hook to interact with the API.
        
        Returns:
            None