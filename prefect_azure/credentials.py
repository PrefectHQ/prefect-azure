import os
from dataclasses import dataclass
from typing import Optional

from azure.storage.blob import BlobServiceClient

HELP_URL = (
    "https://docs.microsoft.com/en-us/azure/storage/blobs/"
    "storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal"
)


@dataclass
class AzureCredentials:
    f"""
    Dataclass used to manage authentication with Azure. Azure authentication is
    handled via the `azure` module. Refer to the [azure docs]({HELP_URL})
    for more info about retrieving the connection string.
    Args:
        azure_connection_string
    """

    azure_connection_string: Optional[str] = None

    def get_blob_service_client(self):
        """
        Returns an authenticated blob service client that can be used to create clients
        for Azure services
        Example:
            Create an authorized blob service session
            >>> azure_connection_string = Secret("azure").get()
            >>> azure_credentials = AzureCredentials(
            >>>     azure_connection_string=azure_connection_string,
            >>> )
            >>> blob_service_client = azure_credentials.get_blob_service_client()
        """
        connection_string = self.azure_connection_string or os.getenv(
            "AZURE_STORAGE_CONNECTION_STRING"
        )
        if connection_string is None:
            raise ValueError(
                f"Either the azure_connection_string must be specified or the "
                f"environment variable AZURE_STORAGE_CONNECTION_STRING must be set;"
                f"see {HELP_URL} for more information."
            )
        try:
            return BlobServiceClient.from_connection_string(connection_string)
        except ValueError:
            raise ValueError(
                f"The azure_connection_string is malformed; please see {HELP_URL} "
                f"for retrieving your connection string."
            )
