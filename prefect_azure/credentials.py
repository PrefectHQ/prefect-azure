from dataclasses import dataclass

from azure.storage.blob.aio import BlobServiceClient

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
        connection_string
    """

    connection_string: str

    def get_blob_service_client(self):
        """
        Returns an authenticated blob service client that can be used to create clients
        for Azure services
        Example:
            Create an authorized blob service session
            >>> connection_string = Secret("azure").get()
            >>> azure_credentials = AzureCredentials(
            >>>     connection_string=connection_string,
            >>> )
            >>> blob_service_client = azure_credentials.get_blob_service_client()
        """
        connection_string = self.connection_string
        try:
            return BlobServiceClient.from_connection_string(connection_string)
        except ValueError:
            raise ValueError(
                f"Please visit {HELP_URL} for retrieving the proper connection string."
            )
