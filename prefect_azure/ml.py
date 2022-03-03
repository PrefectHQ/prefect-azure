import os
from typing import TYPE_CHECKING, Dict, List, Optional, Union

from azureml.core.dataset import Dataset
from azureml.core.datastore import Datastore
from azureml.data import DataType, FileDataset, TabularDataset
from azureml.data.azure_storage_datastore import AzureBlobDatastore
from azureml.data.data_reference import DataReference
from azureml.data.dataset_type_definitions import PromoteHeadersBehavior
from prefect import task

if TYPE_CHECKING:
    from prefect_azure.credentials import MlAzureCredentials


@task
def ml_list_datastore(azure_credentials: MlAzureCredentials) -> Dict:
    """
    Lists the Datastores in the Workspace.

    Args:
        azure_credentials: Credentials to use for authentication with Azure.
    """
    workspace = azure_credentials.get_client()
    results = workspace.datastores
    return results


def ml_get_datastore(azure_credentials: MlAzureCredentials, datastore_name: str = None):
    """
    Gets the Datastore within the Workspace.

    Args:
        azure_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
    """
    workspace = azure_credentials.get_client()
    if datastore_name is None:
        return Datastore.get_default(workspace)
    else:
        result = Datastore.get(workspace, datastore_name=datastore_name)
        return result


@task
def ml_upload_datastore(
    path: Union[str, List[str]],
    azure_credentials: MlAzureCredentials,
    target_path: str = None,
    relative_root: str = None,
    datastore_name: str = None,
    overwrite: bool = False,
) -> DataReference:
    """
    Uploads local files to a Datastore.

    Args:
        path: The path to a single file, single directory,
            or a list of path to files to be uploaded.
        azure_credentials: Credentials to use for authentication with Azure.
        target_path: The location in the blob container to upload to. If
            None, then upload to root.
        relative_root: The root from which is used to determine the path of
            the files in the blob. For example, if we upload /path/to/file.txt,
            and we define base path to be /path, when file.txt is uploaded
            to the blob storage, it will have the path of /to/file.txt.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        overwrite: Overwrite existing file(s).
    """
    datastore = ml_get_datastore(azure_credentials, datastore_name)

    if isinstance(path, str) and os.path.isdir(path):
        data_reference = datastore.upload(
            src_dir=path,
            target_path=target_path,
            overwrite=overwrite,
            show_progress=False,
        )
    elif isinstance(path, str):
        files = [path]
        data_reference = datastore.upload_files(
            files=files,
            relative_root=relative_root,
            target_path=target_path,
            overwrite=overwrite,
            show_progress=False,
        )

    return data_reference


@task
def ml_register_datastore_blob_container(
    container_name: str,
    azure_credentials: MlAzureCredentials,
    datastore_name: str = None,
    create_container_if_not_exists: bool = False,
    overwrite: bool = False,
    set_as_default: bool = False,
) -> AzureBlobDatastore:
    """
    Task for registering Azure Blob Storage container as a
    Datastore in a Azure ML service Workspace.

    Args:
        container_name: The name of the container.
        azure_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the datastore. If not defined, the
            container name will be used.
        create_container_if_not_exists: Create a container, if one does not
            exist with the given name.
        overwrite: Overwrite an existing datastore. If
            the datastore does not exist, it will be created.
        set_as_default: Set the created Datastore as the default datastore
            for the Workspace.
    """
    if datastore_name is None:
        datastore_name = container_name

    workspace = azure_credentials.get_client()

    connection_string = azure_credentials.connection_string
    account_meta = dict([line.split("=", 1) for line in connection_string.split(";")])

    datastore = Datastore.register_azure_blob_container(
        workspace=workspace,
        datastore_name=datastore_name,
        container_name=container_name,
        account_name=account_meta["AccountName"],
        account_key=account_meta["AccountKey"],
        overwrite=overwrite,
        create_if_not_exists=create_container_if_not_exists,
    )

    if set_as_default:
        datastore.set_as_default()

    return datastore


@task
def ml_create_dataset_from_delimited_files(
    dataset_name: str,
    path: Union[str, List[str]],
    azure_credentials: MlAzureCredentials,
    datastore_name: str = None,
    dataset_description: str = "",
    dataset_tags: Optional[Dict[str, str]] = None,
    include_path: bool = False,
    infer_column_types: bool = True,
    set_column_types: Optional[Dict[str, DataType]] = None,
    fine_grain_timestamp: str = None,
    coarse_grain_timestamp: str = None,
    separator: str = ",",
    header: PromoteHeadersBehavior = PromoteHeadersBehavior.ALL_FILES_HAVE_SAME_HEADERS,
    partition_format: str = None,
    create_new_version: bool = False,
) -> TabularDataset:
    """
    Task for creating a TabularDataset from delimited files
    for use in a Azure Machine Learning service Workspace.
    The files should exist in a Datastore.

    Args:
        dataset_name: The name of the Dataset in the Workspace.
        path: The path to the delimited files in the Datastore.
        azure_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        include_path: Boolean to keep path information as column in the dataset.
        infer_column_types: Boolean to infer column data types.
        set_column_types: A dictionary to set column data type,
            where key is column name and value is a `azureml.data.DataType`.
        fine_grain_timestamp: The name of column as fine grain timestamp.
        coarse_grain_timestamp: The name of column coarse grain timestamp.
        separator: The separator used to split columns.
        header: Controls how column headers are promoted when reading from files.
            Defaults to assume that all files have the same header.
        partition_format: Specify the partition format of path.
        create_new_version: Boolean to register the dataset as a new
            version under the specified name.

    Returns:
        azureml.data.TabularDataset: the created TabularDataset
    """
    if isinstance(path, str):
        path = [path]

    dataset_tags = dataset_tags or dict()

    datastore = ml_get_datastore(azure_credentials, datastore_name)

    dataset = Dataset.Tabular.from_delimited_files(
        path=[(datastore, p) for p in path],
        include_path=include_path,
        infer_column_types=infer_column_types,
        set_column_types=set_column_types,
        separator=separator,
        header=header,
        partition_format=partition_format,
    )

    dataset = dataset.with_timestamp_columns(
        fine_grain_timestamp=fine_grain_timestamp,
        coarse_grain_timestamp=coarse_grain_timestamp,
        validate=True,
    )

    dataset = dataset.register(
        workspace=datastore.workspace,
        name=dataset_name,
        description=dataset_description,
        tags=dataset_tags,
        create_new_version=create_new_version,
    )

    return dataset


@task
def ml_create_dataset_from_parquet_files(
    dataset_name: str,
    path: Union[str, List[str]],
    azure_credentials: MlAzureCredentials,
    datastore_name: str = None,
    dataset_description: str = "",
    dataset_tags: Optional[Dict[str, str]] = None,
    include_path: bool = False,
    set_column_types: Optional[Dict[str, DataType]] = None,
    fine_grain_timestamp: str = None,
    coarse_grain_timestamp: str = None,
    partition_format: str = None,
    create_new_version: bool = None,
) -> TabularDataset:
    """
    Task for creating a TabularDataset from Parquet files
    for use in a Azure Machine Learning service Workspace.
    The files should exist in a Datastore.

    Args:
        dataset_name: The name of the Dataset in the Workspace.
        path: The path to the delimited files in the Datastore.
        azure_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        include_path: Boolean to keep path information as column in the dataset.
        set_column_types: A dictionary to set column data type,
            where key is column name and value is a `azureml.data.DataType`.
        fine_grain_timestamp: The name of column as fine grain timestamp.
        coarse_grain_timestamp: The name of column coarse grain timestamp.
        partition_format: Specify the partition format of path.
        create_new_version: Boolean to register the dataset as a new version
            under the specified name.

    Returns:
        azureml.data.TabularDataset: the created TabularDataset.
    """
    if isinstance(path, str):
        path = [path]

    dataset_tags = dataset_tags or dict()

    datastore = ml_get_datastore(azure_credentials, datastore_name)

    dataset = Dataset.Tabular.from_parquet_files(
        path=[(datastore, p) for p in path],
        include_path=include_path,
        set_column_types=set_column_types,
        partition_format=partition_format,
    )

    dataset = dataset.with_timestamp_columns(
        fine_grain_timestamp=fine_grain_timestamp,
        coarse_grain_timestamp=coarse_grain_timestamp,
        validate=True,
    )

    dataset = dataset.register(
        workspace=datastore.workspace,
        name=dataset_name,
        description=dataset_description,
        tags=dataset_tags,
        create_new_version=create_new_version,
    )

    return dataset


@task
def ml_create_dataset_from_files(
    dataset_name: str,
    path: Union[str, List[str]],
    azure_credentials: MlAzureCredentials,
    datastore_name: str = None,
    dataset_description: str = "",
    dataset_tags: Optional[Dict[str, str]] = None,
    create_new_version: bool = False,
) -> FileDataset:
    """
    Task for creating a FileDataset from files for
    use in a Azure Machine Learning service Workspace.
    The files should exist in a Datastore.

    Args:
        dataset_name: The name of the Dataset in the Workspace
        path: The path to the delimited files in the Datastore.
        azure_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        create_new_version: Boolean to register the dataset as a new
            version under the specified name.

    Returns:
        azureml.data.FileDataset: the created FileDataset
    """
    if not isinstance(path, list):
        path = [path]

    dataset_tags = dataset_tags or dict()

    datastore = ml_get_datastore(azure_credentials, datastore_name)

    dataset = Dataset.File.from_files(path=[(datastore, p) for p in path])

    dataset = dataset.register(
        workspace=datastore.workspace,
        name=dataset_name,
        description=dataset_description,
        tags=dataset_tags,
        create_new_version=create_new_version,
    )

    return dataset
