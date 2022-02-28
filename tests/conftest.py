from unittest.mock import MagicMock

import pytest
from prefect.utilities.testing import AsyncMock

from prefect_azure.credentials import BlobStorageAzureCredentials


class AsyncIter:
    def __init__(self, items):
        self.items = items

    async def __aiter__(self):
        for item in self.items:
            yield item


class ClientMock(MagicMock):
    download_blob = AsyncMock()
    download_blob.return_value.content_as_bytes = AsyncMock(
        return_value=b"prefect_works"
    )

    upload_blob = AsyncMock()
    upload_blob.return_value = "prefect.txt"

    list_blobs = MagicMock()
    list_blobs.return_value = AsyncIter(range(5))


@pytest.fixture
def blob_storage_azure_credentials():
    return BlobStorageAzureCredentials("connection_string")


@pytest.fixture
def blob_service_client_mock(monkeypatch):
    BlobServiceClientMock = MagicMock()
    BlobServiceClientMock.from_connection_string().get_blob_client.return_value = (
        ClientMock()
    )
    BlobServiceClientMock.from_connection_string().get_container_client.return_value = (
        ClientMock()
    )
    monkeypatch.setattr(
        "prefect_azure.credentials.BlobServiceClient", BlobServiceClientMock
    )
