# prefect-azure

Visit the full docs [here](https://PrefectHQ.github.io/prefect-azure) to see additional examples and the API reference.

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-azure/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-azure?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-azure/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-azure?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-azure/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-azure?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-azure/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-azure?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

prefect-azure is a collection of prebuilt Prefect tasks that can be used to quickly construct Prefect flows.

## Getting Started

### Python setup

Requires an installation of Python 3.7+

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-azure` with `pip`

```bash
pip install prefect-azure
```

To use Blob Storage:
```bash
pip install "prefect-azure[blob_storage]"
```

To use Cosmos DB:
```bash
pip install "prefect-azure[cosmos_db]"
```

To use ML Datastore:
```bash
pip install "prefect-azure[ml_datastore]"
```

A list of available blocks in `prefect-azure` and their setup instructions can be found [here](https://PrefectHQ.github.io/prefect-azure/#blocks-catalog).

### Download a blob

```python
from prefect import flow

from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_download

@flow
def example_blob_storage_download_flow():
    connection_string = "connection_string"
    blob_storage_credentials = AzureBlobStorageCredentials(
        connection_string=connection_string,
    )
    data = blob_storage_download(
        blob="prefect.txt",
        container="prefect",
        azure_credentials=blob_storage_credentials,
    )
    return data

example_blob_storage_download_flow()
```

Use `with_options` to customize options on any existing task or flow:
```python
custom_blob_storage_download_flow = example_blob_storage_download_flow.with_options(
    name="My custom task name",
    retries=2,
    retry_delay_seconds=10,
)
```

### Run a command on an Azure container instance

```python
from prefect import flow
from prefect_azure import AzureContainerInstanceCredentials
from prefect_azure.container_instance import AzureContainerInstanceJob


@flow
def container_instance_job_flow():
    aci_credentials = AzureContainerInstanceCredentials.load("MY_BLOCK_NAME")
    container_instance_job = AzureContainerInstanceJob(
        aci_credentials=aci_credentials,
        resource_group_name="azure_resource_group.example.name",
        subscription_id="<MY_AZURE_SUBSCRIPTION_ID>",
        command=["echo", "hello world"],
    )
    return container_instance_job.run()
```

### Use Azure Container Instance as infrastructure

If we have `a_flow_module.py`:

```python
from prefect import flow, get_run_logger

@flow
def log_hello_flow(name="Marvin"):
    logger = get_run_logger()
    logger.info(f"{name} said hello!")

if __name__ == "__main__":
    log_hello_flow()
```

We can run that flow using an Azure Container Instance, but first create the infrastructure block:

```python
from prefect_azure import AzureContainerInstanceCredentials
from prefect_azure.container_instance import AzureContainerInstanceJob

container_instance_job = AzureContainerInstanceJob(
    aci_credentials=AzureContainerInstanceCredentials.load("MY_BLOCK_NAME"),
    resource_group_name="azure_resource_group.example.name",
    subscription_id="<MY_AZURE_SUBSCRIPTION_ID>",
)
container_instance_job.save("aci-dev")
```

Then, create the deployment either on the UI or through the CLI:
```bash
prefect deployment build a_flow_module.py:log_hello_flow --name aci-dev -ib container-instance-job/aci-dev
```

Visit [Prefect Deployments](https://docs.prefect.io/tutorials/deployments/) for more information about deployments.

For more tips on how to use tasks and flows in a Collection, check out [Using Collections](https://orion-docs.prefect.io/collections/usage/)!

## Azure Container Instance Worker
The Azure Container Instance worker is an excellent way to run 
[Prefect projects](https://docs.prefect.io/latest/concepts/projects/) on Azure. 

To get started, create an Azure Container Instances typed work pool:
```
prefect work-pool create -t azure-container-instance my-aci-work-pool
```

Then, run a worker that pulls jobs from the work pool:
```
prefect worker start -n my-aci-worker -p my-aci-work-pool
```

The worker should automatically read the work pool's type and start an 
Azure Container Instance worker.

## Resources

If you encounter and bugs while using `prefect-azure`, feel free to open an issue in the [prefect-azure](https://github.com/PrefectHQ/prefect-azure) repository.

If you have any questions or issues while using `prefect-azure`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack)

Feel free to star or watch [`prefect-azure`](https://github.com/PrefectHQ/prefect-azure) for updates too!

## Contributing

If you'd like to help contribute to fix an issue or add a feature to `prefect-azure`, please [propose changes through a pull request from a fork of the repository](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request-from-a-fork).

Here are the steps:

1. [Fork the repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#forking-a-repository)
2. [Clone the forked repository](https://docs.github.com/en/get-started/quickstart/fork-a-repo#cloning-your-forked-repository)
3. Install the repository and its dependencies:
```
pip install -e ".[dev]"
```
4. Make desired changes
5. Add tests
6. Insert an entry to [CHANGELOG.md](https://github.com/PrefectHQ/prefect-azure/blob/main/CHANGELOG.md)
7. Install `pre-commit` to perform quality checks prior to commit:
```
pre-commit install
```
8. `git commit`, `git push`, and create a pull request
