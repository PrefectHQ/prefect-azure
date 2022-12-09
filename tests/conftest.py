import datetime
from unittest.mock import MagicMock

import pytest
from azure.core.exceptions import ResourceExistsError
from azure.storage.blob import BlobProperties, BlobType
from prefect.testing.utilities import AsyncMock


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

    @property
    def credential(self):
        return MagicMock(account_name="account_name", account_key="account_key")

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
        return AsyncIter(
            [
                BlobProperties(
                    **{
                        "name": "test-blob",
                        "container": "test-container",
                        "snapshot": None,
                        "version_id": None,
                        "is_current_version": None,
                        "blob_type": BlobType.BLOCKBLOB,
                        "metadata": {},
                        "encrypted_metadata": None,
                        "last_modified": datetime.datetime(
                            2022, 12, 9, 18, 29, 44, tzinfo=datetime.timezone.utc
                        ),
                        "etag": "etag",
                        "size": 9,
                        "content_range": None,
                        "append_blob_committed_block_count": None,
                        "is_append_blob_sealed": None,
                        "page_blob_sequence_number": None,
                        "server_encrypted": True,
                        "copy": {
                            "id": None,
                            "source": None,
                            "status": None,
                            "progress": None,
                            "completion_time": None,
                            "status_description": None,
                            "incremental_copy": None,
                            "destination_snapshot": None,
                        },
                        "content_settings": {
                            "content_type": "application/octet-stream",
                            "content_encoding": None,
                            "content_language": None,
                            "content_md5": bytearray(b"1V"),
                            "content_disposition": None,
                            "cache_control": None,
                        },
                        "lease": {
                            "status": "unlocked",
                            "state": "available",
                            "duration": None,
                        },
                        "blob_tier": "Hot",
                        "rehydrate_priority": None,
                        "blob_tier_change_time": None,
                        "blob_tier_inferred": True,
                        "deleted": None,
                        "deleted_time": None,
                        "remaining_retention_days": None,
                        "creation_time": datetime.datetime(
                            2022, 12, 9, 18, 26, 17, tzinfo=datetime.timezone.utc
                        ),
                        "archive_status": None,
                        "encryption_key_sha256": None,
                        "encryption_scope": None,
                        "request_server_encrypted": None,
                        "object_replication_source_properties": [],
                        "object_replication_destination_policy": None,
                        "last_accessed_on": None,
                        "tag_count": None,
                        "tags": None,
                        "immutability_policy": {
                            "expiry_time": None,
                            "policy_mode": None,
                        },
                        "has_legal_hold": None,
                        "has_versions_only": None,
                    }
                )
            ]
        )

    async def close(self):
        return None


@pytest.fixture
def blob_storage_credentials():
    blob_storage_credentials = MagicMock()
    blob_storage_credentials.get_client.side_effect = (
        lambda: BlobStorageClientMethodsMock()
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
    ml_credentials.get_workspace.side_effect = lambda: MagicMock(datastores=["a", "b"])
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


@pytest.fixture
def blob_connection_string():
    return "AccountName=account_name;AccountKey=account_key"


class CosmosClientMock(MagicMock):
    def from_connection_string(connection_string):
        return CosmosClientMock()

    def get_client(self):
        return CosmosClientMock(client="client")

    def get_database_client(self, database):
        return CosmosClientMock(database=database)

    def get_container_client(self, container):
        return CosmosClientMock(container=container)


@pytest.fixture
def cosmos_connection_string(monkeypatch):
    monkeypatch.setattr("prefect_azure.credentials.CosmosClient", CosmosClientMock)
    return "AccountEndpoint=url/;AccountKey=AccountKey==;"
