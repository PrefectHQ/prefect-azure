from . import _version
from .credentials import (  # noqa
    AzureBlobStorageCredentials,
    AzureCosmosDbCredentials,
    AzureMlCredentials,
    ACICredentials
)
from .aci import ACITask

__all__ = ["AzureBlobStorageCredentials", "AzureCosmosDbCredentials", "AzureMlCredentials", "ACICredentials", "ACITask"]

__version__ = _version.get_versions()["version"]
