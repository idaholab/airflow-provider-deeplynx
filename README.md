# DeepLynx-Airflow-Provider

Airflow documentation on creating a custom provider [here](https://airflow.apache.org/docs/apache-airflow-providers/howto/create-custom-providers.html)

airflow-provider-sample project [here](https://github.com/astronomer/airflow-provider-sample)


## Notes on decision to not store token and token expiry
- (background) Airflow is designed so that all tasks in a given dag should be able to run [independently](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/tasks.html)
- I originally wanted integrate token generation, storage, and updating in the DeepLynx Hook, making things easier for users. This was working out nicely until I realized its also bad practice to update values in Airflow's Connections table via a dag run. Updating an Airflow Variable in a dag run could be viable, but since we anticipate a given dag to potentially have many users (possibly deeplynx airflow-app service users that register a given "workflow instance") keeping track of those Variables could be an issue. ["I agree that Variables are a useful tool, but when you have k=v pairs that you only want to use for a single run, it gets complicated and messy."](https://stackoverflow.com/questions/57062998/is-it-possible-to-update-overwrite-the-airflow-dag-run-conf)
- By not storing the generated token it makes many things much simpler. As long as a user generates a token (with long enough expiration) at the beginning of their dag, they can just pass that to subsequent tasks as needed using [XComs](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html).
- Basically, I want to keep it simple and this is following Airflow best practices. ["The tasks should also not store any authentication parameters such as passwords or token inside them. Where at all possible, use Connections to store data securely in Airflow backend and retrieve them using a unique connection id."](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#communication)
