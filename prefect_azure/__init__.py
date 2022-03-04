from . import _version
from .credentials import (  # noqa
    BlobStorageAzureCredentials,
    CosmosDbAzureCredentials,
    MlAzureCredentials,
)

__version__ = _version.get_versions()["version"]
