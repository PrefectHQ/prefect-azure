from . import _version
from .credentials import (  # noqa
    AzureBlobStorageCredentials,
    AzureCosmosDbCredentials,
    AzureMlCredentials,
    AzureContainerInstanceCredentials,
)
from .workers.container_instance import AzureContainerWorker  # noqa
from .container_instance import AzureContainerInstanceJob  # noqa

__all__ = [
    "AzureBlobStorageCredentials",
    "AzureCosmosDbCredentials",
    "AzureMlCredentials",
    "AzureContainerInstanceCredentials",
    "AzureContainerInstanceJob",
    "AzureContainerWorker",
]

__version__ = _version.get_versions()["version"]
