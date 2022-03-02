from typing import Dict, List, Optional, Union

import azureml.core.dataset
from azureml.core.datastore import Datastore
from azureml.data import DataType, TabularDataset
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
    self,
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
