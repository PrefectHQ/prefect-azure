"""Tasks for interacting with Azure Cosmos DB"""

from typing import Any, Dict, List, Optional, Union

from azure.cosmos.database import ContainerProxy, DatabaseProxy
from prefect import task
from prefect.logging import get_run_logger

from prefect_azure.credentials import AzureCredentials


@task
def query_items(
    query: str,
    container: Union[str, ContainerProxy, Dict[str, Any]],
    database: Union[str, DatabaseProxy, Dict[str, Any]],
    azure_credentials: AzureCredentials,
    parameters: Optional[List[Dict[str, object]]] = None,
    **kwargs: Any
) -> List[Union[str, dict]]:
    """
    Return all results matching the given query.

    You can use any value for the container name in the FROM clause,
    but often the container name is used.
    In the examples below, the container name is "products,"
    and is aliased as "p" for easier referencing in the WHERE clause.

    Args:
        query: The Azure Cosmos DB SQL query to execute.
        container: Name of the Cosmos DB container to query from.
        azure_credentials: Credentials to use for authentication with Azure.
        parameters: Optional array of parameters to the query.
            Each parameter is a dict() with 'name' and 'value' keys.
        **kwargs: Additional keyword arguments to pass to `query_items`.

    Returns:
        An `list` of results.

    Example:
        Query SampleDB Persons container where age >= 44
        ```
        from prefect import flow

        from prefect_azure import AzureCredentials
        from prefect_azure.cosmos_db import query_items


        @flow
        def example_cosmos_db_query_items_flow():
            query = "SELECT * FROM c where c.age >= @age"
            container = "Persons"
            database = "SampleDB"
            parameters = [dict(name="@age", value=44)]

            results = query_items(
                query,
                container,
                database,
                azure_credentials,
                parameters=parameters,
                enable_cross_partition_query=True,
            )
            return results
        ```
    """
    logger = get_run_logger()
    logger.info("Running query from container %s in %s database", container, database)

    cosmos_client = azure_credentials.get_cosmos_client()
    database_client = cosmos_client.get_database_client(database)
    container_client = database_client.get_container_client(container)
    results = list(container_client.query_items(query, parameters=parameters, **kwargs))
    return results
