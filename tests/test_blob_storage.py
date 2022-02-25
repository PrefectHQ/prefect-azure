from unittest.mock import MagicMock

from prefect import flow
from prefect.utilities.testing import AsyncMock

from prefect_azure.blob_storage import (
    blob_storage_download,
    blob_storage_list,
    blob_storage_upload,
)


class ClientMock(MagicMock):
    download_blob = AsyncMock()
    download_blob.return_value.content_as_bytes = AsyncMock(
        return_value=b"prefect_works"
    )

    upload_blob = AsyncMock()
    upload_blob.return_value = "prefect.txt"

    list_blobs = AsyncMock()
    list_blobs.return_value = ["prefect.txt"]


async def test_blob_storage_download_flow(monkeypatch, azure_credentials):
    BlobServiceClientMock = MagicMock()
    BlobServiceClientMock.from_connection_string().get_blob_client.return_value = (
        ClientMock()
    )

    monkeypatch.setattr(
        "prefect_azure.credentials.BlobServiceClient", BlobServiceClientMock
    )

    @flow
    async def blob_storage_download_flow():
        return await blob_storage_download(
            blob="prefect.txt",
            container="prefect",
            azure_credentials=azure_credentials,
        )

    data = (await blob_storage_download_flow()).result().result()
    assert data.decode() == "prefect_works"


async def test_blob_storage_upload_flow(monkeypatch, azure_credentials):
    BlobServiceClientMock = MagicMock()
    BlobServiceClientMock.from_connection_string().get_blob_client.return_value = (
        ClientMock()
    )

    monkeypatch.setattr(
        "prefect_azure.credentials.BlobServiceClient", BlobServiceClientMock
    )

    @flow
    async def blob_storage_upload_flow():
        return await blob_storage_upload(
            "prefect_works",
            blob="prefect.txt",
            container="prefect",
            azure_credentials=azure_credentials,
        )

    blob = (await blob_storage_upload_flow()).result().result()
    assert blob == "prefect.txt"


async def test_blob_storage_list_flow(monkeypatch, azure_credentials):
    BlobServiceClientMock = MagicMock()
    BlobServiceClientMock.from_connection_string().get_container_client.return_value = (
        ClientMock()
    )

    monkeypatch.setattr(
        "prefect_azure.credentials.BlobServiceClient", BlobServiceClientMock
    )

    @flow
    async def blob_storage_list_flow():
        return await blob_storage_list(
            container="prefect",
            azure_credentials=azure_credentials,
        )

    blobs = (await blob_storage_list_flow()).result().result()
    assert blobs == ["prefect.txt"]
