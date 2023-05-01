import os
from pathlib import Path
from unittest.mock import ANY, MagicMock, call

import pytest
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

from prefect_azure.projects.steps import (
    pull_project_from_azure_blob_storage,
    push_project_to_azure_blob_storage,
)


@pytest.fixture
def container_client_mock():
    return MagicMock(spec=ContainerClient)


@pytest.fixture
def default_azure_credential_mock():
    return MagicMock(spec=DefaultAzureCredential)


@pytest.fixture
def mock_azure_blob_storage(
    monkeypatch, container_client_mock, default_azure_credential_mock
):
    monkeypatch.setattr(
        "prefect_azure.projects.steps.ContainerClient",
        container_client_mock,
    )
    monkeypatch.setattr(
        "prefect_azure.projects.steps.DefaultAzureCredential",
        default_azure_credential_mock,
    )


@pytest.fixture
def tmp_files(tmp_path: Path):
    files = [
        "testfile1.txt",
        "testfile2.txt",
        "testfile3.txt",
        "testdir1/testfile4.txt",
        "testdir2/testfile5.txt",
    ]

    (tmp_path / ".prefectignore").write_text(
        """
    testdir1/*
    .prefectignore
    """
    )

    for file in files:
        filepath = tmp_path / file
        filepath.parent.mkdir(parents=True, exist_ok=True)
        filepath.write_text("Sample text")

    return tmp_path


@pytest.mark.usefixtures("mock_azure_blob_storage")
def test_push_project_to_azure_blob_storage_with_connection_string(
    tmp_files: Path, container_client_mock: MagicMock
):
    container = "test-container"
    folder = "test-folder"
    credentials = {"connection_string": "fake_connection_string"}

    os.chdir(tmp_files)

    push_project_to_azure_blob_storage(container, folder, credentials)

    container_client_mock.from_connection_string.assert_called_once_with(
        credentials["connection_string"], container_name=container
    )

    upload_blob_mock = (
        container_client_mock.from_connection_string.return_value.__enter__.return_value.upload_blob  # noqa
    )

    upload_blob_mock.assert_has_calls(
        [
            call(
                f"{folder}/testfile1.txt",
                ANY,
                overwrite=True,
            ),
            call(
                f"{folder}/testfile2.txt",
                ANY,
                overwrite=True,
            ),
            call(
                f"{folder}/testfile3.txt",
                ANY,
                overwrite=True,
            ),
            call(
                f"{folder}/testdir2/testfile5.txt",
                ANY,
                overwrite=True,
            ),
        ],
        any_order=True,
    )

    assert all(
        [
            open(call.args[1].name).read() == "Sample text"
            for call in upload_blob_mock.call_args_list
        ]
    )


@pytest.mark.usefixtures("mock_azure_blob_storage")
def test_pull_project_from_azure_blob_storage_with_connection_string(
    tmp_path, container_client_mock
):
    container = "test-container"
    folder = "test-folder"
    credentials = {"connection_string": "fake_connection_string"}

    blob_mock = MagicMock()
    blob_mock.name = f"{folder}/sample_file.txt"

    mock_context_client = (
        container_client_mock.from_connection_string.return_value.__enter__.return_value
    )
    mock_context_client.list_blobs.return_value = [blob_mock]

    pull_project_from_azure_blob_storage(container, folder, credentials)

    mock_context_client.list_blobs.assert_called_once_with(name_starts_with=folder)
    mock_context_client.download_blob.assert_called_once_with(blob_mock)

    expected_file = tmp_path / "sample_file.txt"
    assert expected_file.exists()
