from dataclasses import dataclass

from azure.cosmos import CosmosClient
from azure.storage.blob.aio import BlobServiceClient
from prefect.logging import get_run_logger

HELP_URLS = {
    "blob_storage": "https://docs.microsoft.com/en-us/azure/storage/blobs/"
    "storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal",
    "cosmos_db": "https://docs.microsoft.com/en-us/azure/cosmos-db/sql/"
    "create-sql-api-python#update-your-connection-string",
}
HELP_FMT = "Please visit {help_url} for retrieving the proper connection string."


@dataclass
class AzureCredentials:
    """
    Dataclass used to manage authentication with Azure. Azure authentication is
    handled via the `azure` module, primarily through a connection string.

    Args:
        connection_string
    """

    connection_string: str

    def get_client(self) -> None:
        raise NotImplementedError(
            "This is an abstract class and has no implementation of get_client"
        )


class BlobStorageAzureCredentials(AzureCredentials):
    def get_client(self) -> BlobServiceClient:
        """
        Returns an authenticated Blob Service client that can be used to create clients
        for Azure services.

        Example:
            Create an authorized Blob Service session
            ```python
            import os
            from prefect_azure import BlobStorageAzureCredentials

            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            azure_credentials = BlobStorageAzureCredentials(
                connection_string=connection_string,
            )
            blob_service_client = azure_credentials.get_client()
            ```
        """
        logger = get_run_logger()
        logger.info("Creating a Blob Storage service client")

        try:
            return BlobServiceClient.from_connection_string(self.connection_string)
        except ValueError:
            logger.exception(HELP_FMT.format(help_url=HELP_URLS["blob_storage"]))
            raise


class CosmosDbAzureCredentials(AzureCredentials):
    def get_client(self) -> CosmosClient:
        """
        Returns an authenticated Cosmos DB client that can be used to query databases
        for Azure services.

        Example:
            Create an authorized Cosmos session
            ```python
            import os
            from prefect_azure import BlobStorageAzureCredentials

            connection_string = os.getenv("AZURE_COSMOS_CONNECTION_STRING")
            azure_credentials = CosmosDbAzureCredentials(
                connection_string=connection_string,
            )
            cosmos_db_client = azure_credentials.get_client()
            ```
        """
        logger = get_run_logger()
        logger.info("Creating a Cosmos DB service client")

        try:
            return CosmosClient.from_connection_string(self.connection_string)
        except ValueError:
            logger.exception(HELP_FMT.format(help_url=HELP_URLS["cosmos_db"]))
            raise
