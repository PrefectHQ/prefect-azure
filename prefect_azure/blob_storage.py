"""Integrations for interacting with Azure Blob Storage"""

from io import BytesIO
from pathlib import Path
import uuid
from typing import TYPE_CHECKING, Any, BinaryIO, Coroutine, Dict, List, Optional, Union

if TYPE_CHECKING:
    from azure.storage.blob import BlobProperties

from prefect import task
from prefect.logging import get_run_logger
from prefect.blocks.abstract import ObjectStorageBlock
from prefect.filesystems import WritableDeploymentStorage, WritableFileSystem
from prefect.utilities.asyncutils import sync_compatible
from pydantic import VERSION as PYDANTIC_VERSION
from prefect.utilities.filesystem import filter_files

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field


from prefect_azure.credentials import AzureBlobStorageCredentials


@task
async def blob_storage_download(
    container: str,
    blob: str,
    blob_storage_credentials: "AzureBlobStorageCredentials",
) -> bytes:
    """
    Downloads a blob with a given key from a given Blob Storage container.
    Args:
        blob: Name of the blob within this container to retrieve.
        container: Name of the Blob Storage container to retrieve from.
        blob_storage_credentials: Credentials to use for authentication with Azure.
    Returns:
        A `bytes` representation of the downloaded blob.
    Example:
        Download a file from a Blob Storage container
        ```python
        from prefect import flow

        from prefect_azure import AzureBlobStorageCredentials
        from prefect_azure.blob_storage import blob_storage_download

        @flow
        def example_blob_storage_download_flow():
            connection_string = "connection_string"
            blob_storage_credentials = AzureBlobStorageCredentials(
                connection_string=connection_string,
            )
            data = blob_storage_download(
                container="prefect",
                blob="prefect.txt",
                blob_storage_credentials=blob_storage_credentials,
            )
            return data

        example_blob_storage_download_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Downloading blob from container %s with key %s", container, blob)

    async with blob_storage_credentials.get_blob_client(container, blob) as blob_client:
        blob_obj = await blob_client.download_blob()
        output = await blob_obj.content_as_bytes()

    return output


@task
async def blob_storage_upload(
    data: bytes,
    container: str,
    blob_storage_credentials: "AzureBlobStorageCredentials",
    blob: str = None,
    overwrite: bool = False,
) -> str:
    """
    Uploads data to an Blob Storage container.
    Args:
        data: Bytes representation of data to upload to Blob Storage.
        container: Name of the Blob Storage container to upload to.
        blob_storage_credentials: Credentials to use for authentication with Azure.
        blob: Name of the blob within this container to retrieve.
        overwrite: If `True`, an existing blob with the same name will be overwritten.
            Defaults to `False` and an error will be thrown if the blob already exists.
    Returns:
        The blob name of the uploaded object
    Example:
        Read and upload a file to a Blob Storage container
        ```python
        from prefect import flow

        from prefect_azure import AzureBlobStorageCredentials
        from prefect_azure.blob_storage import blob_storage_upload

        @flow
        def example_blob_storage_upload_flow():
            connection_string = "connection_string"
            blob_storage_credentials = AzureBlobStorageCredentials(
                connection_string=connection_string,
            )
            with open("data.csv", "rb") as f:
                blob = blob_storage_upload(
                    data=f.read(),
                    container="container",
                    blob="data.csv",
                    blob_storage_credentials=blob_storage_credentials,
                    overwrite=False,
                )
            return blob

        example_blob_storage_upload_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Uploading blob to container %s with key %s", container, blob)

    # create key if not provided
    if blob is None:
        blob = str(uuid.uuid4())

    async with blob_storage_credentials.get_blob_client(container, blob) as blob_client:
        await blob_client.upload_blob(data, overwrite=overwrite)

    return blob


@task
async def blob_storage_list(
    container: str,
    blob_storage_credentials: "AzureBlobStorageCredentials",
    name_starts_with: str = None,
    include: Union[str, List[str]] = None,
    **kwargs,
) -> List["BlobProperties"]:
    """
    List objects from a given Blob Storage container.
    Args:
        container: Name of the Blob Storage container to retrieve from.
        blob_storage_credentials: Credentials to use for authentication with Azure.
        name_starts_with: Filters the results to return only blobs whose names
            begin with the specified prefix.
        include: Specifies one or more additional datasets to include in the response.
            Options include: 'snapshots', 'metadata', 'uncommittedblobs', 'copy',
            'deleted', 'deletedwithversions', 'tags', 'versions', 'immutabilitypolicy',
            'legalhold'.
        **kwargs: Addtional kwargs passed to `ContainerClient.list_blobs()`
    Returns:
        A `list` of `dict`s containing metadata about the blob.
    Example:
        ```python
        from prefect import flow

        from prefect_azure import AzureBlobStorageCredentials
        from prefect_azure.blob_storage import blob_storage_list

        @flow
        def example_blob_storage_list_flow():
            connection_string = "connection_string"
            blob_storage_credentials = AzureBlobStorageCredentials(
                connection_string="connection_string",
            )
            data = blob_storage_list(
                container="container",
                blob_storage_credentials=blob_storage_credentials,
            )
            return data

        example_blob_storage_list_flow()
        ```
    """
    logger = get_run_logger()
    logger.info("Listing blobs from container %s", container)

    async with blob_storage_credentials.get_container_client(
        container
    ) as container_client:
        blobs = [
            blob
            async for blob in container_client.list_blobs(
                name_starts_with=name_starts_with, include=include, **kwargs
            )
        ]

    return blobs


class AzureBlobStorageContainer(
    ObjectStorageBlock, WritableFileSystem, WritableDeploymentStorage
):
    _block_type_name = "Azure Blob Storage Container"
    _logo_url = "https://cdn.sanity.io/images/3ugk85nk/production/54e3fa7e00197a4fbd1d82ed62494cb58d08c96a-250x250.png"

    container_name: str = Field(
        default=..., description="The name of a Azure Blob Storage container."
    )
    credentials: AzureBlobStorageCredentials = Field(
        default_factory=AzureBlobStorageCredentials,
        description="The credentials to use for authentication with Azure.",
    )
    base_folder: Optional[str] = Field(
        default=None,
        description=(
            "A base path to a folder within the container to use "
            "for reading and writing objects."
        ),
    )

    def _get_path_relative_to_base_folder(self, path: Optional[str] = None) -> str:
        if path is None and self.base_folder is None:
            return ""
        if path is None:
            return self.base_folder
        if self.base_folder is None:
            return path
        return (Path(self.base_folder) / Path(path)).as_posix()

    @sync_compatible
    async def download_folder_to_path(
        self, from_folder: str, to_folder: str | Path, **download_kwargs: Dict[str, Any]
    ) -> Coroutine[Any, Any, Path]:
        logger = get_run_logger()
        logger.info(
            "Downloading folder from container %s to path %s",
            self.container_name,
            to_folder,
        )
        full_container_path = self._get_path_relative_to_base_folder(from_folder)
        async with self.credentials.get_container_client(
            self.container_name
        ) as container_client:
            async for blob in container_client.list_blobs(
                name_starts_with=full_container_path
            ):
                blob_path = blob.name
                local_path = Path(to_folder) / Path(blob_path).relative_to(
                    full_container_path
                )
                local_path.parent.mkdir(parents=True, exist_ok=True)
                async with container_client.get_blob_client(blob_path) as blob_client:
                    blob_obj = await blob_client.download_blob()
                    await blob_obj.download_to_path(local_path)
        return Path(to_folder)

    @sync_compatible
    async def download_object_to_file_object(
        self,
        from_path: str,
        to_file_object: BinaryIO,
        **download_kwargs: Dict[str, Any],
    ) -> Coroutine[Any, Any, BinaryIO]:
        logger = get_run_logger()
        logger.info(
            "Downloading object from container %s to file object", self.container_name
        )
        full_container_path = self._get_path_relative_to_base_folder(from_path)
        async with self.credentials.get_blob_client(
            self.container_name, full_container_path
        ) as blob_client:
            blob_obj = await blob_client.download_blob()
            await blob_obj.download_to_stream(to_file_object)
        return to_file_object

    @sync_compatible
    async def download_object_to_path(
        self, from_path: str, to_path: str | Path, **download_kwargs: Dict[str, Any]
    ) -> Coroutine[Any, Any, Path]:
        logger = get_run_logger()
        logger.info(
            "Downloading object from container %s to path %s",
            self.container_name,
            to_path,
        )
        full_container_path = self._get_path_relative_to_base_folder(from_path)
        async with self.credentials.get_blob_client(
            self.container_name, full_container_path
        ) as blob_client:
            blob_obj = await blob_client.download_blob()
            await blob_obj.download_to_path(to_path)
        return Path(to_path)

    @sync_compatible
    async def upload_from_file_object(
        self, from_file_object: BinaryIO, to_path: str, **upload_kwargs: Dict[str, Any]
    ) -> Coroutine[Any, Any, str]:
        logger = get_run_logger()
        logger.info(
            "Uploading object to container %s with key %s", self.container_name, to_path
        )
        full_container_path = self._get_path_relative_to_base_folder(to_path)
        async with self.credentials.get_blob_client(
            self.container_name, full_container_path
        ) as blob_client:
            await blob_client.upload_blob(from_file_object, **upload_kwargs)
        return to_path

    @sync_compatible
    async def upload_from_path(
        self, from_path: str | Path, to_path: str, **upload_kwargs: Dict[str, Any]
    ) -> Coroutine[Any, Any, str]:
        logger = get_run_logger()
        logger.info(
            "Uploading object to container %s with key %s", self.container_name, to_path
        )
        full_container_path = self._get_path_relative_to_base_folder(to_path)
        async with self.credentials.get_blob_client(
            self.container_name, full_container_path
        ) as blob_client:
            await blob_client.upload_blob(from_path, **upload_kwargs)
        return to_path

    @sync_compatible
    async def upload_from_folder(
        self, from_folder: str | Path, to_folder: str, **upload_kwargs: Dict[str, Any]
    ) -> Coroutine[Any, Any, str]:
        logger = get_run_logger()
        logger.info(
            "Uploading folder to container %s with key %s",
            self.container_name,
            to_folder,
        )
        full_container_path = self._get_path_relative_to_base_folder(to_folder)
        async with self.credentials.get_container_client(
            self.container_name
        ) as container_client:
            for path in Path(from_folder).rglob("*"):
                if path.is_file():
                    blob_path = Path(full_container_path) / path.relative_to(
                        from_folder
                    )
                    async with container_client.get_blob_client(
                        blob_path.as_posix()
                    ) as blob_client:
                        await blob_client.upload_blob(path, **upload_kwargs)
        return full_container_path

    @sync_compatible
    async def get_directory(
        self, from_path: str = None, local_path: str = None
    ) -> None:
        await self.download_folder_to_path(from_path, local_path)

    @sync_compatible
    async def put_directory(
        self, local_path: str = None, to_path: str = None, ignore_file: str = None
    ) -> None:
        to_path = "" if to_path is None else to_path

        if local_path is None:
            local_path = "."

        included_files = None
        if ignore_file:
            with open(ignore_file, "r") as f:
                ignore_patterns = f.readlines()

            included_files = filter_files(local_path, ignore_patterns)

        for local_file_path in Path(local_path).expanduser().rglob("*"):
            if (
                included_files is not None
                and str(local_file_path.relative_to(local_path)) not in included_files
            ):
                continue
            elif not local_file_path.is_dir():
                remote_file_path = Path(to_path) / local_file_path.relative_to(
                    local_path
                )
                with open(local_file_path, "rb") as local_file:
                    local_file_content = local_file.read()

                await self.write_path(
                    remote_file_path.as_posix(), content=local_file_content
                )

    @sync_compatible
    async def read_path(self, path: str) -> bytes:
        file_obj = BytesIO()
        await self.download_object_to_file_object(path, file_obj)
        return file_obj.get_value()

    @sync_compatible
    async def write_path(self, path: str, content: bytes) -> None:
        await self.upload_from_file_object(BytesIO(content), path)
