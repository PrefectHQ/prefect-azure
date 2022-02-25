"""Tasks for interacting with Azure Blob Storage"""
import uuid

from prefect import task
from prefect.logging import get_run_logger

from prefect_azure.credentials import AzureCredentials


def _get_blob_client(azure_credentials, blob, container):
    """
    Helper to get the blob client.
    """
    blob_service_client = azure_credentials.get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(blob=blob, container=container)
    return blob_client


@task
async def blob_storage_download(
    blob: str,
    container: str,
    azure_credentials: AzureCredentials,
) -> bytes:
    """
    Downloads an object with a given key from a given Blob Storage container.
    Args:
        blob: Name of the blob within this container to retrieve.
        container: Name of the Blob Storage container to retrieve from.
        azure_credentials: Credentials to use for authentication with Azure.
    Returns:
        A `bytes` representation of the downloaded object.
    Example:
        Download a file from a blob container
        ```python
        from prefect import flow

        from prefect_azure import AzureCredentials
        from prefect_azure.blob_storage import blob_storage_download

        @flow
        async def example_blob_storage_download_flow():
            azure_credentials = AzureCredentials(
                connection_string="connection_string",
            )
            data = await blob_storage_download(
                blob="prefect.txt",
                container="prefect",
                azure_credentials=azure_credentials,
            )
            return data

        await example_blob_storage_download_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading object from container %s with key %s", container, blob)

    blob_client = _get_blob_client(azure_credentials, blob, container)
    blob_obj = await blob_client.download_blob()
    output = await blob_obj.content_as_bytes()

    return output


@task
async def blob_storage_upload(
    data: bytes,
    blob: str,
    container: str,
    azure_credentials: AzureCredentials,
    overwrite: bool = False,
) -> str:
    """
    Uploads data to an Blob Storage container.
    Args:
        data: Bytes representation of data to upload to Blob Storage.
        blob: Name of the blob within this container to retrieve.
        container: Name of the Blob Storage container to upload to.
        azure_credentials: Credentials to use for authentication with Azure.
        overwrite: If `True`, an existing blob with the same name will be overwritten.
            Defaults to `False` and an error will be thrown if the blob already exists.
    Returns:
        The blob name of the uploaded object
    Example:
        Read and upload a file to a Blob Storage container
        ```python
        from prefect import flow

        from prefect_azure import AzureCredentials
        from prefect_azure.blob_storage import blob_storage_upload

        @flow
        async def example_blob_storage_upload_flow():
            azure_credentials = AzureCredentials(
                connection_string="connection_string",
            )
            with open("data.csv", "rb") as f:
                blob = await blob_storage_upload(
                    data=f.read(),
                    blob="data.csv",
                    container="container",
                    azure_credentials=azure_credentials,
                    overwrite=False,
                )
            return blob
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading object to container %s with key %s", container, blob)

    # create key if not provided
    if blob is None:
        blob = str(uuid.uuid4())

    blob_client = _get_blob_client(azure_credentials, blob, container)
    await blob_client.upload_blob(data, overwrite=overwrite)

    return blob


@task
async def blob_storage_list(
    container: str,
    azure_credentials: AzureCredentials,
) -> list:
    """
    List objects from a given Blob Storage container.
    Args:
        container: Name of the Blob Storage container to retrieve from.
        azure_credentials: Credentials to use for authentication with Azure.
    Returns:
        A `list` of `dict`s containing metadata about the blob.
    Example:
        ```python
        from prefect import flow

        from prefect_azure import AzureCredentials
        from prefect_azure.blob_storage import blob_storage_list

        @flow
        async def example_blob_storage_list_flow():
            azure_credentials = AzureCredentials(
                connection_string=connection_string,
            )
            data = await blob_storage_list(
                container="container",
                azure_credentials=azure_credentials,
            )
            return data
        ```
    """
    logger = get_run_logger()
    logger.info("Listing blobs from container %s", container)

    blob_service_client = azure_credentials.get_blob_service_client()
    container_client = blob_service_client.get_container_client(container)

    blobs = [blob async for blob in container_client.list_blobs()]
    return blobs
