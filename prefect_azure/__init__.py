from . import _version
from .credentials import BlobStorageAzureCredentials, CosmosDbAzureCredentials  # noqa

__all__ = ["BlobStorageAzureCredentials", "CosmosDbAzureCredentials"]

__version__ = _version.get_versions()["version"]
