# prefect-azure

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

Then, register to [view the block](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_azure
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

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

### Run a command on an Azure container instance

```python
from prefect import flow
from prefect_azure import ContainerInstanceCredentials
from prefect_azure.container_instance import ContainerInstanceJob

@flow
def container_instance_job_flow():
    aci_credentials = ContainerInstanceCredentials.load("MY_BLOCK_NAME")
    container_instance_job = ContainerInstanceJob(
        aci_credentials=aci_credentials,
        resource_group_name="azure_resource_group.example.name",
        command=["echo", "hello world"],
    )
    return container_instance_job.run()
```

### Use Azure Container Instance as infrastructure

If we have a `a_flow_module.py`:

```python
from prefect import flow, get_run_logger

@flow
def log_hello_flow(name):
    logger = get_run_logger()
    logger.info(f"{name} said hello!")
```

We can run that flow using an Azure Container Instance, but first create the deployment:

```python
from prefect_azure import ContainerInstanceCredentials
from prefect_azure.container_instance import ContainerInstanceJob

container_instance_job = ContainerInstanceJob(
    namespace="dev",
    aci_credentials=ContainerInstanceCredentials.load("MY_BLOCK_NAME"),
    resource_group_name="azure_resource_group.example.name",
)
container_instance_job.save("aci_dev")
```

Create the deployment either on the UI or through the CLI:
```bash
prefect deployment build a_flow_module.py:log_hello_flow --name aci_dev -ib container-instance-job/aci_dev
```

Visit [Prefect Deployments](https://docs.prefect.io/tutorials/deployments/) for more information about deployments.


## Resources

If you encounter and bugs while using `prefect-azure`, feel free to open an issue in the [prefect-azure](https://github.com/PrefectHQ/prefect-azure) repository.

If you have any questions or issues while using `prefect-azure`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack)

Feel free to ⭐️ or watch [`prefect-azure`](https://github.com/PrefectHQ/prefect-azure) for updates too!

## Development

If you'd like to install a version of `prefect-azure` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-azure.git

cd prefect-azure/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
