# DeepLynx-Airflow-Provider

## Installation
### Install from PyPI
To install the provider package from PyPI, simply run:

```sh
pip install airflow-provider-deeplynx
```
### Install locally
- Clone the repository to your local machine.
- Navigate to the cloned repository directory: `cd airflow-provider-deeplynx`
- Install the package using pip: `pip install .`
  - for development, you can install in editable mode with `pip install -e .`

## Usage
Most communication with DeepLynx requires a bearer token, so the first task of a DeepLynx dag is usually to generate a token, which can be done with `GetOauthTokenOperator`. Once a token is generated using this operator, it can be passed to downstream tasks using [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html#xcoms), the token generation `task_id`, and the key `token`. `GetOauthTokenOperator` requires either a `conn_id` of an Airflow [Connection](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/connections.html#connections-hooks) of type DeepLynx, or the parameters `host`, `api_key`, and `api_secret`. It is recommended to create a new Airflow connection of type DeepLynx through the [Airflow UI](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html#creating-a-connection-with-the-ui), and input values for `DeepLynx URL`, `API Key`, and `API Secret`. You can then use this DeepLynx connection's id to set the
`conn_id` for any airflow operators in this package (alternatively, you can supply the `host` parameter). Most functionality can be understood by looking at the provided [Example Dags](deeplynx_provider\example_dags). Class level documentation is also provided.

### Example Dags
Example dags are provided in [`deeplynx_provider\example_dags`](deeplynx_provider\example_dags). Copy the full directory into your airflow [`DAG_FOLDER`](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#loading-dags) to have them loaded into your airflow environment.

#### Functional Test
A single, self-contained DAG functional test. Its mainly meant for new users to test their installation and to quickly see how to use the package.

- create container
- import container
- set data source active
- import data
- query data
- upload result
- download file

### DeepLynx Operator Required Inputs
<!-- TODO: want to use a documentation system to generate basic docs for my hook and operators, and then point to that documentation from this section -->

### DeepLynx Config
Communication with DeepLynx using this package can be configured with various options like ssl cert and local file writing locations. Most of the time, the default deeplynx config will work just fine, but to learn more continue reading.

The operators in this provider package use the [Deep Lynx Python SDK](https://github.com/idaholab/Deep-Lynx-Python-Package) to communicate with DeepLynx. The [DeepLynxConfigurationOperator](deeplynx_provider\operators\configuration_operator.py) can be used to set your (Configuration)[https://github.com/idaholab/Deep-Lynx-Python-Package/blob/main/deep_lynx/configuration.py] exactly how you want it, and this configuration is then passed to a task instance (XCom)[https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html] so that downstream tasks derived from [DeepLynxBaseOperator](deeplynx_provider\operators\deeplynx_base_operator.py) can use this configuration.

<!-- TODO: need to explain ssl_ca_cert and set_temp_folder and other things that can be set with env vars -->
Default DeepLynx Configuration:
  -


## Notes
### Other Documentation
- Development setup [here](https://github.inl.gov/Digital-Engineering/airflow-dev)
- Airflow documentation on creating a custom provider [here](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html)
- airflow-provider-sample project [here](https://github.com/astronomer/airflow-provider-sample)

### Decision to not store token and token expiry
- (background) Airflow is designed so that all tasks in a given dag should be able to run [independently](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
- I originally wanted integrate token generation, storage, and updating in the DeepLynx Hook, making things easier for users. This was working out nicely until I realized its also bad practice to update values in Airflow's Connections table via a dag run. Updating an Airflow Variable in a dag run could be viable, but since we anticipate a given dag to potentially have many users (possibly deeplynx airflow-app service users that register a given "workflow instance") keeping track of those Variables could be an issue. ["I agree that Variables are a useful tool, but when you have k=v pairs that you only want to use for a single run, it gets complicated and messy."](https://stackoverflow.com/questions/57062998/is-it-possible-to-update-overwrite-the-airflow-dag-run-conf)
- By not storing the generated token it makes many things much simpler. As long as a user generates a token (with long enough expiration) at the beginning of their dag, they can just pass that to subsequent tasks as needed using [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html).
- Basically, I want to keep it simple and this is following Airflow best practices. ["The tasks should also not store any authentication parameters such as passwords or token inside them. Where at all possible, use Connections to store data securely in Airflow backend and retrieve them using a unique connection id."](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#communication)
