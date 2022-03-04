from conftest import BlobStorageClientMethodsMock
from prefect import flow


def test_blob_storage_credentials_get_client(blob_storage_azure_credentials):
    @flow
    def test_flow():
        return blob_storage_azure_credentials.get_client()

    assert isinstance(test_flow().result(), BlobStorageClientMethodsMock)
