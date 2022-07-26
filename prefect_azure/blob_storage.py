"""Tasks for interacting with Azure Blob Storage"""
import uuid
from typing import TYPE_CHECKING, List

if TYPE_CHECKING:
    from azure.storage.blob import BlobProperties

from prefect import task
from prefect.logging import get_run_logger

if TYPE_CHECKING:
    from prefect_azure.credentials import AzureBlobStorageCredentials


@task
async def blob_storage_download(
    container: str,
    blob: str,
    blob_storage_credentials: "AzureBlobStorageCredentials",
) -> bytes:
    """
    Downloads a blob with a given key from a given Blob Storage container.
    Args:
        blob: Name of the blob within this container to retrieve.
        container: Name of the Blob Storage container to retrieve from.
        blob_storage_credentials: Credentials to use for authentication with Azure.
    Returns:
        A `bytes` representation of the downloaded blob.
    Example:
        Download a file from a Blob Storage container
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
                container="prefect",
                blob="prefect.txt",
                blob_storage_credentials=blob_storage_credentials,
            )
            return data

        example_blob_storage_download_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading blob from container %s with key %s", container, blob)

    async with blob_storage_credentials.get_blob_client(container, blob) as blob_client:
        blob_obj = await blob_client.download_blob()
        output = await blob_obj.content_as_bytes()

    return output


@task
async def blob_storage_upload(
    data: bytes,
    container: str,
    blob_storage_credentials: "AzureBlobStorageCredentials",
    blob: str = None,
    overwrite: bool = False,
) -> str:
    """
    Uploads data to an Blob Storage container.
    Args:
        data: Bytes representation of data to upload to Blob Storage.
        container: Name of the Blob Storage container to upload to.
        blob_storage_credentials: Credentials to use for authentication with Azure.
        blob: Name of the blob within this container to retrieve.
        overwrite: If `True`, an existing blob with the same name will be overwritten.
            Defaults to `False` and an error will be thrown if the blob already exists.
    Returns:
        The blob name of the uploaded object
    Example:
        Read and upload a file to a Blob Storage container
        ```python
        from prefect import flow

        from prefect_azure import AzureBlobStorageCredentials
        from prefect_azure.blob_storage import blob_storage_upload

        @flow
        def example_blob_storage_upload_flow():
            connection_string = "connection_string"
            blob_storage_credentials = AzureBlobStorageCredentials(
                connection_string=connection_string,
            )
            with open("data.csv", "rb") as f:
                blob = blob_storage_upload(
                    data=f.read(),
                    container="container",
                    blob="data.csv",
                    blob_storage_credentials=blob_storage_credentials,
                    overwrite=False,
                )
            return blob

        example_blob_storage_upload_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob to container %s with key %s", container, blob)

    # create key if not provided
    if blob is None:
        blob = str(uuid.uuid4())

    async with blob_storage_credentials.get_blob_client(container, blob) as blob_client:
        await blob_client.upload_blob(data, overwrite=overwrite)

    return blob


@task
async def blob_storage_list(
    container: str,
    blob_storage_credentials: "AzureBlobStorageCredentials",
) -> List["BlobProperties"]:
    """
    List objects from a given Blob Storage container.
    Args:
        container: Name of the Blob Storage container to retrieve from.
        blob_storage_credentials: Credentials to use for authentication with Azure.
    Returns:
        A `list` of `dict`s containing metadata about the blob.
    Example:
        ```python
        from prefect import flow

        from prefect_azure import AzureBlobStorageCredentials
        from prefect_azure.blob_storage import blob_storage_list

        @flow
        def example_blob_storage_list_flow():
            connection_string = "connection_string"
            blob_storage_credentials = AzureBlobStorageCredentials(
                connection_string="connection_string",
            )
            data = blob_storage_list(
                container="container",
                blob_storage_credentials=blob_storage_credentials,
            )
            return data

        example_blob_storage_list_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Listing blobs from container %s", container)

    async with blob_storage_credentials.get_container_client(
        container
    ) as container_client:
        blobs = [blob async for blob in container_client.list_blobs()]

    return blobs
