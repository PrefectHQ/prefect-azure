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
def blob_storage_credentials():
    blob_storage_credentials = MagicMock(
        connection_string="AccountName=Name;AccountKey=Key"
    )
    blob_storage_credentials.get_blob_client.side_effect = (
        lambda container, blob: BlobStorageClientMethodsMock(blob)
    )
    blob_storage_credentials.get_container_client.side_effect = (
        lambda container: BlobStorageClientMethodsMock()
    )
    return blob_storage_credentials


class CosmosDbClientMethodsMock:
    def query_items(self, *args, **kwargs):
        return [{"name": "Someone", "age": 23}]

    def read_item(self, *args, **kwargs):
        return {"name": "Someone", "age": 23}

    def create_item(self, *args, **kwargs):
        return {"name": "Other", "age": 3}


@pytest.fixture
def cosmos_db_credentials():
    cosmos_db_credentials = MagicMock()
    cosmos_db_credentials.get_container_client.side_effect = (
        lambda container, database: CosmosDbClientMethodsMock()
    )
    return cosmos_db_credentials


@pytest.fixture
def ml_credentials():
    ml_credentials = MagicMock()
    ml_credentials.get_client.side_effect = lambda: MagicMock(datastores=["a", "b"])
    return ml_credentials


class DatastoreMethodsMock:
    def __init__(self, workspace, datastore_name="default"):
        self.workspace = workspace
        self.datastore_name = datastore_name

    def upload(self, *args, **kwargs):
        return kwargs

    def upload_files(self, *args, **kwargs):
        return kwargs


@pytest.fixture
def datastore(monkeypatch):
    DatastoreMock = MagicMock()
    DatastoreMock.get_default.side_effect = lambda workspace: DatastoreMethodsMock(
        workspace
    )
    DatastoreMock.get.side_effect = (
        lambda workspace, datastore_name: DatastoreMethodsMock(
            workspace, datastore_name=datastore_name
        )
    )
    DatastoreMock.register_azure_blob_container.side_effect = (
        lambda **kwargs: "registered"
    )

    monkeypatch.setattr("prefect_azure.ml_datastore.Datastore", DatastoreMock)
