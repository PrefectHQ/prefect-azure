import os
from typing import Dict, List, Optional, Union

import azureml.core.dataset
import azureml.core.datastore
from azureml.core.datastore import Datastore
from azureml.core.workspace import Workspace
from azureml.data import DataType, TabularDataset
from azureml.data.azure_storage_datastore import AzureBlobDatastore
from azureml.data.data_reference import DataReference
from azureml.data.dataset_type_definitions import PromoteHeadersBehavior
from prefect import task


@task
def ml_create_dataset_from_delimited_files(
    dataset_name: str,
    datastore: Datastore,
    path: Union[str, List[str]],
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
        dataset_name: The name of the Dataset in the Workspace
        datastore: The Datastore which holds the files.
        path: The path to the delimited files in the Datastore.
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
    if not isinstance(path, list):
        path = [path]

    dataset_tags = dataset_tags or dict()

    dataset = azureml.core.dataset.Dataset.Tabular.from_delimited_files(
        path=[(datastore, path_item) for path_item in path],
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
    datastore: Datastore,
    path: Union[str, List[str]],
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
        dataset_name: The name of the Dataset in the Workspace
        datastore: The Datastore which holds the
          files.
        path: The path to the delimited files in the Datastore.
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
    if not isinstance(path, list):
        path = [path]

    dataset_tags = dataset_tags or dict()

    dataset = azureml.core.dataset.Dataset.Tabular.from_parquet_files(
        path=[(datastore, path_item) for path_item in path],
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
    datastore: Datastore,
    path: Union[str, List[str]],
    dataset_description: str = "",
    dataset_tags: Optional[Dict[str, str]] = None,
    create_new_version: bool = False,
) -> azureml.data.FileDataset:
    """
    Task for creating a FileDataset from files for
    use in a Azure Machine Learning service Workspace.
    The files should exist in a Datastore.

    Args:
        dataset_name: The name of the Dataset in the Workspace
        datastore: The Datastore which holds
          the files.
        path: The path to the delimited files in the
          Datastore.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        create_new_version: Boolean to register the dataset as a new
          version under the specified name.'

    Returns:
        azureml.data.FileDataset: the created FileDataset
    """
    if not isinstance(path, list):
        path = [path]

    dataset_tags = dataset_tags or dict()

    dataset = azureml.core.dataset.Dataset.File.from_files(
        path=[(datastore, path_item) for path_item in path]
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
def ml_register_datastore_blob_container(
    workspace: Workspace,
    container_name: str,
    datastore_name: str = None,
    create_container_if_not_exists: bool = False,
    overwrite_existing_datastore: bool = False,
    azure_credentials_secret: str = "AZ_CREDENTIALS",
    set_as_default: bool = False,
) -> AzureBlobDatastore:
    """
    Task for registering Azure Blob Storage container as a
    Datastore in a Azure ML service Workspace.

    Args:
        workspace: The Workspace to which the Datastore is
            to be registered.
        container_name: The name of the container.
        datastore_name: The name of the datastore. If not defined, the
            container name will be used.
        create_container_if_not_exists: Create a container, if one does not
            exist with the given name.
        overwrite_existing_datastore: Overwrite an existing datastore. If
            the datastore does not exist, it will be created.
        azure_credentials_secret: The name of the Prefect Secret that stores
            your Azure credentials; this Secret must be a JSON string with two keys:
            `ACCOUNT_NAME` and either `ACCOUNT_KEY` or `SAS_TOKEN` (if both are defined
            then`ACCOUNT_KEY` is used).
        set_as_default: Set the created Datastore as the default datastore
            for the Workspace.
        **kwargs: additional keyword arguments to pass to the Task
            constructor
    """
    if datastore_name is None:
        datastore_name = container_name

    # get Azure credentials
    azure_credentials = "PLACEHOLDER<<<<<<<<<<<"
    az_account_name = azure_credentials["ACCOUNT_NAME"]
    az_account_key = azure_credentials.get("ACCOUNT_KEY")
    az_sas_token = azure_credentials.get("SAS_TOKEN")

    datastore = azureml.core.datastore.Datastore.register_azure_blob_container(
        workspace=workspace,
        datastore_name=datastore_name,
        container_name=container_name,
        account_name=az_account_name,
        account_key=az_account_key,
        sas_token=az_sas_token,
        overwrite=overwrite_existing_datastore,
        create_if_not_exists=create_container_if_not_exists,
    )

    if set_as_default:
        datastore.set_as_default()

    return datastore


@task
def ml_list_datastore(workspace: Workspace):
    """
    Task for listing the Datastores in a Workspace.
    Args:
        workspace: The Workspace which Datastores are to
            be listed.
        **kwargs: additional keyword arguments to pass to the Task
            constructor
    """
    return workspace.datastores


def ml_get_datastore(workspace: Workspace, datastore_name: str = None):
    """
    Task for getting a Datastore registered to a given Workspace.
    Args:
        workspace: The Workspace which Datastore is retrieved.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        **kwargs: additional keyword arguments to pass to the Task
            constructor
    """
    if datastore_name is None:
        return azureml.core.datastore.Datastore.get_default(workspace)

    return azureml.core.datastore.Datastore.get(
        workspace, datastore_name=datastore_name
    )


@task
def ml_upload_datastore(
    self,
    datastore: azureml.core.datastore.Datastore = None,
    path: Union[str, List[str]] = None,
    relative_root: str = None,
    target_path: str = None,
    overwrite: bool = False,
) -> DataReference:
    """
    Task for uploading local files to a Datastore.
    Args:
        datastore: The datastore to upload the files to.
        relative_root: The root from which is used to determine the path of
            the files in the blob. For example, if we upload /path/to/file.txt,
            and we define base path to be /path, when file.txt is uploaded
            to the blob storage, it will have the path of /to/file.txt.
        path: The path to a single file, single directory,
            or a list of path to files to eb uploaded.
        target_path: The location in the blob container to upload to. If
            None, then upload to root.
        overwrite: Overwrite existing file(s).
        **kwargs: additional keyword arguments to pass to the Task constructor
    """
    if isinstance(path, str) and os.path.isdir(path):
        data_reference = datastore.upload(
            src_dir=path,
            target_path=target_path,
            overwrite=overwrite,
            show_progress=False,
        )
        return data_reference

    if isinstance(path, str):
        path = [path]

    data_reference = datastore.upload_files(
        files=path,
        relative_root=relative_root,
        target_path=target_path,
        overwrite=overwrite,
        show_progress=False,
    )

    return data_reference
