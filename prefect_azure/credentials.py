from dataclasses import dataclass

from azure.cosmos import CosmosClient
from azure.storage.blob.aio import BlobServiceClient

HELP_URLS = {
    "blob_service": "https://docs.microsoft.com/en-us/azure/storage/blobs/"
    "storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal",
    "cosmos": "https://docs.microsoft.com/en-us/azure/cosmos-db/sql/"
    "create-sql-api-python#update-your-connection-string",
}


@dataclass
class AzureCredentials:
    """
    Dataclass used to manage authentication with Azure. Azure authentication is
    handled via the `azure` module, primarily through a connection string.

    Args:
        connection_string
    """

    connection_string: str

    def get_blob_service_client(self) -> BlobServiceClient:
        """
        Returns an authenticated blob service client that can be used to create clients
        for Azure services.

        Example:
            Create an authorized blob service session
            ```python
            connection_string = Secret("azure_blob_service").get()
            azure_credentials = AzureCredentials(
                connection_string=connection_string,
            )
            blob_service_client = azure_credentials.get_blob_service_client()
            ```
        """
        connection_string = self.connection_string
        try:
            return BlobServiceClient.from_connection_string(connection_string)
        except ValueError:
            raise ValueError(
                f"Please visit {HELP_URLS['blob_service']} for retrieving "
                f"the proper connection string."
            )

    def get_cosmos_client(self) -> CosmosClient:
        """
        Returns an authenticated Cosmos client that can be used to query databases
        for Azure services.

        Example:
            Create an authorized Cosmos session
            ```python
            connection_string = Secret("azure_cosmos").get()
            azure_credentials = AzureCredentials(
                connection_string=connection_string,
            )
            cosmos_client = azure_credentials.get_cosmos_client()
            ```
        """
        connection_string = self.connection_string
        try:
            return CosmosClient.from_connection_string(connection_string)
        except ValueError:
            raise ValueError(
                f"Please visit {HELP_URLS['cosmos']} for retrieving "
                f"the proper connection string."
            )
