"""Tasks for interacting with Azure ML Dataset"""

from typing import TYPE_CHECKING, Dict, List, Optional, Union

from azureml.core.dataset import Dataset
from azureml.data.dataset_type_definitions import PromoteHeadersBehavior
from prefect import get_run_logger, task

from prefect_azure.credentials import ml_get_datastore

if TYPE_CHECKING:
    from azureml.core.datastore import Datastore
    from azureml.data import DataType, FileDataset, TabularDataset

    from prefect_azure.credentials import MlAzureCredentials


def _postprocess_dataset(
    dataset: Dataset,
    fine_grain_timestamp: str,
    coarse_grain_timestamp: str,
    datastore: "Datastore",
    dataset_name: str,
    dataset_description: str,
    dataset_tags: Optional[Dict[str, str]],
    create_new_version: bool,
) -> Dataset:
    """
    Helper method to postprocess created dataset for
    subsequent functions to reduce redundancy.
    """
    dataset = dataset.with_timestamp_columns(
        fine_grain_timestamp=fine_grain_timestamp,
        coarse_grain_timestamp=coarse_grain_timestamp,
        validate=True,
    )

    dataset = dataset.register(
        workspace=datastore.workspace,
        name=dataset_name,
        description=dataset_description,
        tags=dataset_tags or dict(),
        create_new_version=create_new_version,
    )
    return dataset


@task
def ml_create_dataset_from_delimited_files(
    dataset_name: str,
    path: Union[str, List[str]],
    ml_credentials: "MlAzureCredentials",
    datastore_name: str = None,
    include_path: bool = False,
    infer_column_types: bool = True,
    set_column_types: Optional[Dict[str, DataType]] = None,
    separator: str = ",",
    header: PromoteHeadersBehavior = PromoteHeadersBehavior.ALL_FILES_HAVE_SAME_HEADERS,
    partition_format: str = None,
    fine_grain_timestamp: str = None,
    coarse_grain_timestamp: str = None,
    dataset_description: str = "",
    dataset_tags: Optional[Dict[str, str]] = None,
    create_new_version: bool = False,
) -> TabularDataset:
    """
    Creates a TabularDataset from delimited files
    for use in a Azure Machine Learning service Workspace.
    The files should exist in a Datastore.

    Args:
        dataset_name: The name of the Dataset in the Workspace.
        path: The path to the delimited files in the Datastore.
        ml_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        include_path: Boolean to keep path information as column in the dataset.
        infer_column_types: Boolean to infer column data types.
        set_column_types: A dictionary to set column data type,
            where key is column name and value is a `azureml.data.DataType`.
        separator: The separator used to split columns.
        header: Controls how column headers are promoted when reading from files.
            Defaults to assume that all files have the same header.
        partition_format: Specify the partition format of path.
        fine_grain_timestamp: The name of column as fine grain timestamp.
        coarse_grain_timestamp: The name of column coarse grain timestamp.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        create_new_version: Boolean to register the dataset as a new
            version under the specified name.

    Example:
        ```python
        import os

        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml import *

        @flow
        def example_create_dataset_from_delimited_files_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            results = ml_create_dataset_from_delimited_files(
                "dataset_name",
                "path",
                "ml_credentials",
                datastore_name="datastore_name"
            )
            return results
        ```

    Returns:
        The created TabularDataset
    """
    logger = get_run_logger()
    logger.info(
        "Creating %s dataset from delimited files from %s in %s datastore",
        dataset_name,
        path,
        datastore_name,
    )

    datastore = ml_get_datastore(ml_credentials, datastore_name)

    if isinstance(path, str):
        path = [path]

    dataset = Dataset.Tabular.from_delimited_files(
        path=[(datastore, p) for p in path],
        include_path=include_path,
        infer_column_types=infer_column_types,
        set_column_types=set_column_types,
        separator=separator,
        header=header,
        partition_format=partition_format,
    )

    dataset = _postprocess_dataset(
        dataset,
        fine_grain_timestamp,
        coarse_grain_timestamp,
        datastore,
        dataset_name,
        dataset_description,
        dataset_tags,
        create_new_version,
    )
    return dataset


@task
def ml_create_dataset_from_parquet_files(
    dataset_name: str,
    path: Union[str, List[str]],
    ml_credentials: "MlAzureCredentials",
    datastore_name: str = None,
    include_path: bool = False,
    set_column_types: Optional[Dict[str, DataType]] = None,
    partition_format: str = None,
    fine_grain_timestamp: str = None,
    coarse_grain_timestamp: str = None,
    create_new_version: bool = None,
    dataset_description: str = "",
    dataset_tags: Optional[Dict[str, str]] = None,
) -> TabularDataset:
    """
    Creates a TabularDataset from Parquet files
    for use in a Azure Machine Learning service Workspace.
    The files should exist in a Datastore.

    Args:
        dataset_name: The name of the Dataset in the Workspace.
        path: The path to the delimited files in the Datastore.
        ml_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        include_path: Boolean to keep path information as column in the dataset.
        set_column_types: A dictionary to set column data type,
            where key is column name and value is a `azureml.data.DataType`.
        partition_format: Specify the partition format of path.
        fine_grain_timestamp: The name of column as fine grain timestamp.
        coarse_grain_timestamp: The name of column coarse grain timestamp.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        create_new_version: Boolean to register the dataset as a new version
            under the specified name.

    Example:
        ```python
        import os

        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml import *

        @flow
        def example_ml_create_dataset_from_parquet_files_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            results = ml_create_dataset_from_parquet_files(
                "dataset_name",
                "path",
                "ml_credentials",
                datastore_name="datastore_name"
            )
            return results
        ```

    Returns:
        The created TabularDataset.
    """
    datastore = ml_get_datastore(ml_credentials, datastore_name)

    if isinstance(path, str):
        path = [path]

    dataset = Dataset.Tabular.from_parquet_files(
        path=[(datastore, p) for p in path],
        include_path=include_path,
        set_column_types=set_column_types,
        partition_format=partition_format,
    )

    dataset = _postprocess_dataset(
        dataset,
        fine_grain_timestamp,
        coarse_grain_timestamp,
        datastore,
        dataset_name,
        dataset_description,
        dataset_tags,
        create_new_version,
    )
    return dataset


@task
def ml_create_dataset_from_files(
    dataset_name: str,
    path: Union[str, List[str]],
    ml_credentials: "MlAzureCredentials",
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
        ml_credentials: Credentials to use for authentication with Azure.
        datastore_name: The name of the Datastore. If `None`, then the
            default Datastore of the Workspace is returned.
        dataset_description: Description of the Dataset.
        dataset_tags: Tags to associate with the Dataset.
        create_new_version: Boolean to register the dataset as a new
            version under the specified name.

    Example:
        ```python
        import os

        from prefect import flow
        from prefect_azure import MlAzureCredentials
        from prefect_azure.ml import *

        @flow
        def example_ml_create_dataset_from_files_flow():
            ml_credentials = MlAzureCredentials(
                tenant_id="tenant_id",
                service_principal_id="service_principal_id",
                service_principal_password="service_principal_password",
                subscription_id="subscription_id",
                resource_group="resource_group",
                workspace_name="workspace_name",
            )
            results = ml_create_dataset_from_files(
                "dataset_name",
                "path",
                "ml_credentials",
                datastore_name="datastore_name"
            )
            return results
        ```

    Returns:
        The created FileDataset
    """
    datastore = ml_get_datastore(ml_credentials, datastore_name)

    if isinstance(path, str):
        path = [path]

    dataset = Dataset.File.from_files(path=[(datastore, p) for p in path])

    dataset = dataset.register(
        workspace=datastore.workspace,
        name=dataset_name,
        description=dataset_description,
        tags=dataset_tags or dict(),
        create_new_version=create_new_version,
    )

    return dataset
