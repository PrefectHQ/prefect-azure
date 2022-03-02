from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import ResourceExistsError
from prefect.utilities.testing import AsyncMock


class AsyncIter:
    def __init__(self, items):
        self.items = items

    async def __aiter__(self):
        for item in self.items:
            yield item


mock_container = {"prefect.txt": b"prefect_works"}


class BlobStorageClientMethodsMock:
    def __init__(self, blob="prefect.txt"):
        self.blob = blob

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def download_blob(self):
        return AsyncMock(
            content_as_bytes=AsyncMock(return_value=mock_container.get(self.blob))
        )

    async def upload_blob(self, data, overwrite):
        if not overwrite and self.blob in mock_container:
            raise ResourceExistsError("Cannot overwrite existing blob")
        mock_container[self.blob] = data
        return self.blob

    def list_blobs(self):
        return AsyncIter(range(5))

    async def close(self):
        return None


@pytest.fixture
def blob_storage_azure_credentials():
    azure_credentials_mock = MagicMock()
    azure_credentials_mock.get_blob_client.side_effect = (
        lambda container, blob: BlobStorageClientMethodsMock(blob)
    )
    azure_credentials_mock.get_container_client.side_effect = (
        lambda container: BlobStorageClientMethodsMock()
    )
    return azure_credentials_mock


class CosmosDbClientMethodsMock:
    def query_items(self, *args, **kwargs):
        return [{"name": "Someone", "age": 23}]

    def read_item(self, *args, **kwargs):
        return {"name": "Someone", "age": 23}

    def create_item(self, *args, **kwargs):
        return {"name": "Other", "age": 3}


@pytest.fixture
def cosmos_db_azure_credentials():
    azure_credentials_mock = MagicMock()
    azure_credentials_mock.get_container_client.side_effect = (
        lambda container, database: CosmosDbClientMethodsMock()
    )
    return azure_credentials_mock
