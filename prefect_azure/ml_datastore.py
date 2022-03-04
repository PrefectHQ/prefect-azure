"""Tasks for interacting with Azure ML Datastore"""

import os
from functools import partial
from typing import TYPE_CHECKING, Dict, List, Union

from anyio import to_thread
from azureml.core.datastore import Datastore
from prefect import get_run_logger, task

if TYPE_CHECKING:
    from azureml.data.azure_storage_datastore import AzureBlobDatastore
    from azureml.data.data_reference import DataReference

    from prefect_azure.credentials import (
        BlobStorageAzureCredentials,
        MlAzureCredentials,
    )


@task
def ml_list_datastores(ml_credentials: "MlAzureCredentials") -> Dict:
    """
    Lists the Datastores in the Workspace.

    Args:
        ml_credentials: Credentials to use for authentication with Azure.

    Example:
        List Datastore objects
        ```python
        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml_datastore import ml_list_datastores

        @flow
        def example_ml_list_datastores_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            results = ml_list_datastores(ml_credentials)
            return results
        ```
    """
    logger = get_run_logger()
    logger.info("Listing datastores")

    workspace = ml_credentials.get_client()
    results = workspace.datastores
    return results


async def _get_datastore(
    ml_credentials: "MlAzureCredentials", datastore_name: str = None
):
    """
    Helper method for get datastore to prevent Task calling another Task.
    """
    workspace = ml_credentials.get_client()

    if datastore_name is None:
        partial_get = partial(Datastore.get_default, workspace)
    else:
        partial_get = partial(Datastore.get, workspace, datastore_name=datastore_name)

    result = await to_thread.run_sync(partial_get)
    return result


@task
async def ml_get_datastore(
    ml_credentials: "MlAzureCredentials", datastore_name: str = None
) -> Datastore:
    """
    Gets the Datastore within the Workspace.

    Args:
        ml_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.

    Example:
        Get Datastore object
        ```python
        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml_datastore import ml_get_datastore

        @flow
        def example_ml_get_datastore_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            results = ml_get_datastore(ml_credentials, datastore_name="datastore_name")
            return results
        ```
    """
    logger = get_run_logger()
    logger.info("Getting datastore %s", datastore_name)

    result = await _get_datastore(ml_credentials, datastore_name)
    return result


@task
async def ml_upload_datastore(
    path: Union[str, List[str]],
    ml_credentials: "MlAzureCredentials",
    target_path: str = None,
    relative_root: str = None,
    datastore_name: str = None,
    overwrite: bool = False,
) -> "DataReference":
    """
    Uploads local files to a Datastore.

    Args:
        path: The path to a single file, single directory,
            or a list of path to files to be uploaded.
        ml_credentials: Credentials to use for authentication with Azure.
        target_path: The location in the blob container to upload to. If
            None, then upload to root.
        relative_root: The root from which is used to determine the path of
            the files in the blob. For example, if we upload /path/to/file.txt,
            and we define base path to be /path, when file.txt is uploaded
            to the blob storage, it will have the path of /to/file.txt.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        overwrite: Overwrite existing file(s).

    Example:
        Upload Datastore object
        ```python
        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml_datastore import ml_upload_datastore

        @flow
        def example_ml_upload_datastore_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            result = ml_upload_datastore(
                "path/to/dir/or/file",
                ml_credentials,
                datastore_name="datastore_name"
            )
            return result
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading %s into %s datastore", path, datastore_name)

    datastore = await _get_datastore(ml_credentials, datastore_name)

    if isinstance(path, str) and os.path.isdir(path):
        partial_upload = partial(
            datastore.upload,
            src_dir=path,
            target_path=target_path,
            overwrite=overwrite,
            show_progress=False,
        )
    else:
        partial_upload = partial(
            datastore.upload_files,
            files=path if isinstance(path, list) else [path],
            relative_root=relative_root,
            target_path=target_path,
            overwrite=overwrite,
            show_progress=False,
        )

    result = await to_thread.run_sync(partial_upload)
    return result


@task
async def ml_register_datastore_blob_container(
    container_name: str,
    ml_credentials: "MlAzureCredentials",
    blob_storage_credentials: "BlobStorageAzureCredentials",
    datastore_name: str = None,
    create_container_if_not_exists: bool = False,
    overwrite: bool = False,
    set_as_default: bool = False,
) -> "AzureBlobDatastore":
    """
    Registers a Azure Blob Storage container as a
    Datastore in a Azure ML service Workspace.

    Args:
        container_name: The name of the container.
        ml_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the datastore. If not defined, the
            container name will be used.
        create_container_if_not_exists: Create a container, if one does not
            exist with the given name.
        overwrite: Overwrite an existing datastore. If
            the datastore does not exist, it will be created.
        set_as_default: Set the created Datastore as the default datastore
            for the Workspace.

    Example:
        Upload Datastore object
        ```python
        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml_datastore import ml_register_datastore_blob_container

        @flow
        def example_ml_register_datastore_blob_container_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            blob_storage_credentials = BlobStorageAzureCredentials("connection_string")
            result = ml_register_datastore_blob_container(
                "container",
                ml_credentials,
                blob_storage_credentials,
                datastore_name="datastore_name"
            )
            return result
        ```
    """
    logger = get_run_logger()

    if datastore_name is None:
        datastore_name = container_name

    logger.info(
        "Registering %s container into %s datastore", container_name, datastore_name
    )

    workspace = ml_credentials.get_client()

    connection_string = blob_storage_credentials.connection_string
    try:
        account_meta = dict(
            [line.split("=", 1) for line in connection_string.split(";")]
        )
    except Exception:
        logger.exception("Malformed connection string; please ensure it's valid")
        raise

    partial_register = partial(
        Datastore.register_azure_blob_container,
        workspace=workspace,
        datastore_name=datastore_name,
        container_name=container_name,
        account_name=account_meta["AccountName"],
        account_key=account_meta["AccountKey"],
        overwrite=overwrite,
        create_if_not_exists=create_container_if_not_exists,
    )
    result = await to_thread.run_sync(partial_register)

    if set_as_default:
        result.set_as_default()

    return result
