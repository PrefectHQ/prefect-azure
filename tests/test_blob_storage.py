import uuid

from prefect import flow

from prefect_azure.blob_storage import (
    blob_storage_download,
    blob_storage_list,
    blob_storage_upload,
)


async def test_blob_storage_download_flow(
    blob_service_client_mock, blob_storage_azure_credentials
):
    @flow
    async def blob_storage_download_flow():
        return await blob_storage_download(
            blob="prefect.txt",
            container="prefect",
            azure_credentials=blob_storage_azure_credentials,
        )

    data = (await blob_storage_download_flow()).result().result()
    assert data.decode() == "prefect_works"


async def test_blob_storage_upload_flow(
    blob_service_client_mock, blob_storage_azure_credentials
):
    @flow
    async def blob_storage_upload_flow():
        return await blob_storage_upload(
            b"prefect_works",
            blob="prefect.txt",
            container="prefect",
            azure_credentials=blob_storage_azure_credentials,
        )

    blob = (await blob_storage_upload_flow()).result().result()
    assert blob == "prefect.txt"


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


async def test_blob_storage_upload_flow_no_blob(
    blob_service_client_mock, blob_storage_azure_credentials
):
    @flow
    async def blob_storage_upload_flow():
        return await blob_storage_upload(
            b"prefect_works",
            container="prefect",
            azure_credentials=blob_storage_azure_credentials,
        )

    blob = (await blob_storage_upload_flow()).result().result()
    assert is_valid_uuid(blob)


async def test_blob_storage_list_flow(
    blob_service_client_mock, blob_storage_azure_credentials
):
    @flow
    async def blob_storage_list_flow():
        return await blob_storage_list(
            container="prefect",
            azure_credentials=blob_storage_azure_credentials,
        )

    blobs = (await blob_storage_list_flow()).result().result()
    assert blobs == list(range(5))
