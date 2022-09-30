from . import _version
from .credentials import (  # noqa
    AzureBlobStorageCredentials,
    AzureCosmosDbCredentials,
    AzureMlCredentials,
    ContainerInstanceCredentials,
)

__all__ = [
    "AzureBlobStorageCredentials",
    "AzureCosmosDbCredentials",
    "AzureMlCredentials",
    "ContainerInstanceCredentials",
]

__version__ = _version.get_versions()["version"]
