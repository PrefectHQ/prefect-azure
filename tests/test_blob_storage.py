from unittest.mock import MagicMock

from prefect import flow

from prefect_azure import AzureCredentials
from prefect_azure.blob_storage import blob_storage_download


async def test_blob_storage_download_flow(monkeypatch):
    mock_container = {"prefect.txt": b"prefect_works"}
    BlobServiceClientMock = MagicMock()
    BlobServiceClientMock.from_connection_string().get_blob_client.side_effect = (
        lambda container, blob: MagicMock(
            download_blob=lambda: MagicMock(
                content_as_bytes=lambda: mock_container.get(blob)
            ),
            upload_blob=lambda data: mock_container.update({blob: data}),
        )
    )

    monkeypatch.setattr(
        "prefect_azure.credentials.BlobServiceClient", BlobServiceClientMock
    )

    @flow
    async def blob_storage_download_flow():
        data = blob_storage_download(
            blob="prefect.txt",
            container="prefect",
            azure_credentials=AzureCredentials(""),
        )
        return await data

    data = await blob_storage_download_flow()
    data = data.result().result()
    assert data.decode() == "prefect_works"
