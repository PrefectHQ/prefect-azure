import uuid

import pytest
from azure.core.exceptions import ResourceExistsError
from prefect import flow

from prefect_azure.blob_storage import (
    blob_storage_download,
    blob_storage_list,
    blob_storage_upload,
)


async def test_blob_storage_download_flow(blob_storage_credentials):
    @flow
    async def blob_storage_download_flow():
        return await blob_storage_download(
            container="prefect",
            blob="prefect.txt",
            blob_storage_credentials=blob_storage_credentials,
        )

    data = (await blob_storage_download_flow()).result().result()
    assert data.decode() == "prefect_works"


def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False


@pytest.mark.parametrize("blob_expected", [None, "prefect.txt", "prefect_new.txt"])
async def test_blob_storage_upload_flow(blob_expected, blob_storage_credentials):
    @flow
    async def blob_storage_upload_flow():
        return await blob_storage_upload(
            b"prefect_works",
            container="prefect",
            blob=blob_expected,
            overwrite=True,
            blob_storage_credentials=blob_storage_credentials,
        )

    blob_result = (await blob_storage_upload_flow()).result().result()
    if blob_expected is None:
        is_valid_uuid(blob_expected)
    else:
        assert blob_expected == blob_result


async def test_blob_storage_upload_blob_exists_flow(
    blob_storage_credentials,
):
    @flow
    async def blob_storage_upload_flow():
        return await blob_storage_upload(
            b"prefect_works",
            container="prefect",
            blob="prefect.txt",
            overwrite=False,
            blob_storage_credentials=blob_storage_credentials,
        )

    with pytest.raises(ResourceExistsError):
        (await blob_storage_upload_flow()).result().result()


async def test_blob_storage_list_flow(blob_storage_credentials):
    @flow
    async def blob_storage_list_flow():
        return await blob_storage_list(
            container="prefect",
            blob_storage_credentials=blob_storage_credentials,
        )

    blobs = (await blob_storage_list_flow()).result().result()
    assert blobs == list(range(5))
