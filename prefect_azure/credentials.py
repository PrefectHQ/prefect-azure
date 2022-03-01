import abc
from dataclasses import dataclass

from azure.cosmos import ContainerProxy, CosmosClient, DatabaseProxy
from azure.storage.blob.aio import BlobClient, BlobServiceClient
from azure.storage.blob.aio import ContainerClient as BlobContainerClient
from prefect.logging import get_run_logger

HELP_URLS = {
    "blob_storage": "https://docs.microsoft.com/en-us/azure/storage/blobs/"
    "storage-quickstart-blobs-python#copy-your-credentials-from-the-azure-portal",
    "cosmos_db": "https://docs.microsoft.com/en-us/azure/cosmos-db/sql/"
    "create-sql-api-python#update-your-connection-string",
}
HELP_FMT = "Please visit {help_url} for retrieving the proper connection string."


@dataclass
class AzureCredentials(abc.ABC):
    """
    Dataclass used to manage authentication with Azure. Azure authentication is
    handled via the `azure` module, primarily through a connection string.

    Args:
        connection_string
    """

    connection_string: str

    @abc.abstractmethod
    def get_client(self) -> None:
        pass


class BlobStorageAzureCredentials(AzureCredentials):
    def get_client(self) -> BlobServiceClient:
        """
        Returns an authenticated base Blob Service client that can be used to create
        other clients for Azure services.

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
        try:
            return BlobServiceClient.from_connection_string(self.connection_string)
        except ValueError:
            logger.exception(HELP_FMT.format(help_url=HELP_URLS["blob_storage"]))
            raise

    def get_blob_client(self, blob, container) -> BlobClient:
        """
        Returns an authenticated Blob client that can be used to
        download and upload blobs.

        Args:
            blob: Name of the blob within this container to retrieve.
            container: Name of the Blob Storage container to retrieve from.

        Example:
            Create an authorized Blob session
            ```python
            import os
            from prefect_azure import BlobStorageAzureCredentials

            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            azure_credentials = BlobStorageAzureCredentials(
                connection_string=connection_string,
            )
            blob_client = azure_credentials.get_blob_client(blob, container)
            ```
        """
        blob_service_client = self.get_client()
        blob_client = blob_service_client.get_blob_client(
            blob=blob, container=container
        )
        return blob_client

    def get_container_client(self, container) -> BlobContainerClient:
        """
        Returns an authenticated Container client that can be used to create clients
        for Azure services.

        Args:
            container: Name of the Blob Storage container to retrieve from.

        Example:
            Create an authorized Container session
            ```python
            import os
            from prefect_azure import BlobStorageAzureCredentials

            connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
            azure_credentials = BlobStorageAzureCredentials(
                connection_string=connection_string,
            )
            container_client = azure_credentials.get_container_client(container)
            ```
        """
        blob_service_client = self.get_client()
        container_client = blob_service_client.get_container_client(container=container)
        return container_client


class CosmosDbAzureCredentials(AzureCredentials):
    def get_client(self) -> CosmosClient:
        """
        Returns an authenticated Cosmos client that can be used to create
        other clients for Azure services.

        Example:
            Create an authorized Cosmos session
            ```python
            import os
            from prefect_azure import CosmosDbAzureCredentials

            connection_string = os.getenv("AZURE_COSMOS_CONNECTION_STRING")
            azure_credentials = CosmosDbAzureCredentials(
                connection_string=connection_string,
            )
            cosmos_client = azure_credentials.get_client()
            ```
        """
        logger = get_run_logger()
        try:
            return CosmosClient.from_connection_string(self.connection_string)
        except ValueError:
            logger.exception(HELP_FMT.format(help_url=HELP_URLS["cosmos_db"]))
            raise

    def get_database_client(self, database: str) -> DatabaseProxy:
        """
        Returns an authenticated Database client.

        Args:
            database: Name of the database.

        Example:
            Create an authorized Cosmos session
            ```python
            import os
            from prefect_azure import CosmosDbAzureCredentials

            connection_string = os.getenv("AZURE_COSMOS_CONNECTION_STRING")
            azure_credentials = CosmosDbAzureCredentials(
                connection_string=connection_string,
            )
            database_client = azure_credentials.get_database_client(database)
            ```
        """
        cosmos_client = self.get_client()
        database_client = cosmos_client.get_database_client(database=database)
        return database_client

    def get_container_client(self, container: str, database: str) -> ContainerProxy:
        """
        Returns an authenticated Container client used for querying.

        Args:
            container: Name of the Cosmos DB container to retrieve from.
            database: Name of the Cosmos DB database.

        Example:
            Create an authorized Container session
            ```python
            import os
            from prefect_azure import BlobStorageAzureCredentials

            connection_string = os.getenv("AZURE_COSMOS_CONNECTION_STRING")
            azure_credentials = CosmosDbAzureCredentials(
                connection_string=connection_string,
            )
            container_client = azure_credentials.get_container_client(container)
            ```
        """
        database_client = self.get_database_client(database)
        container_client = database_client.get_container_client(container=container)
        return container_client
