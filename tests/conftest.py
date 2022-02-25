import pytest

from prefect_azure.credentials import AzureCredentials


@pytest.fixture
def azure_credentials():
    return AzureCredentials("connection_string")
