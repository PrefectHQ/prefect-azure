"""Tasks for interacting with Azure Blob Storage"""
import io
import uuid
from typing import Any, Dict, List, Optional

from prefect import task
from prefect.logging import get_logger

from prefect_azure.credentials import AzureCredentials


@task
def blob_storage_download(
    blob: str,
    container: str,
    azure_credentials: AzureCredentials,
) -> bytes:
    """
    Downloads an object with a given key from a given blob container.
    Args:
        blob: Name of the blob within this container to retrieve.
        container: Name of the Blob Storage container to retrieve from.
        azure_credentials: Credentials to use for authentication with Azure.
    Returns:
        A `bytes` representation of the downloaded object.
    Example:
        Download a file from an blob container
        >>> @flow
        >>> def example_blob_storage_download_flow():
        >>>     azure_credentials = AzureCredentials(
        >>>         connection_string="connection_string",
        >>>     )
        >>>     data = blob_storage_download(
        >>>         blob="blob",
        >>>         container="container",
        >>>         azure_credentials=azure_credentials,
        >>>     )
    """
    logger = get_logger()
    logger.info("Downloading object from bucket %s with key %s", container, blob)

    blob_service_client = azure_credentials.get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(blob=blob, container=container)
    
    blob_obj = blob_client.download_blob()
    output = blob_obj.content_as_bytes()
    return output


@task
def blob_storage_upload(
    data: bytes,
    blob: str,
    container: str,
    overwrite: bool = False,
    azure_credentials: AzureCredentials,
) -> str:
    """
    Uploads data to an Blob Storage container.
    Args:
        data: Bytes representation of data to upload to Blob Storage.
        blob: Name of the blob within this container to retrieve.
        container: Name of the Blob Storage container to upload to.
        overwrite: If `True`, an existing blob with the same name will be overwritten.
            Defaults to `False` and an error will be thrown if the blob already exists.
        azure_credentials: Credentials to use for authentication with Azure.
    Returns:
        The blob name of the uploaded object
    Example:
        Read and upload a file to a Blob Storage container
        >>> @flow
        >>> def example_blob_storage_download_flow():
        >>>     azure_credentials = AzureCredentials(
        >>>         connection_string="connection_string",
        >>>     )
        >>>     with open("data.csv", "rb") as file:
        >>>         key = blob_storage_upload(
        >>>             container="bucket",
        >>>             blob="data.csv",
        >>>             data=file.read(),
        >>>             azure_credentials=azure_credentials,
        >>>         )
    """
    logger = get_logger()

    logger.info("Uploading object to bucket %s with key %s", container, blob)

    blob_service_client = azure_credentials.get_blob_service_client()

    # create key if not provided
    if blob is None:
        blob = str(uuid.uuid4())

    blob_client = blob_service_client.get_blob_client(blob=blob, container=container)
    blob_client.upload_blob(data, overwrite=overwrite)

    return blob