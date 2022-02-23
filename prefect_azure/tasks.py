from prefect import task


@task
def hello_prefect_azure() -> str:
    """
    Sample task that says hello!

    Returns:
        A greeting for your collection
    """
    return "Hello, prefect-azure!"


@task
def goodbye_prefect_azure() -> str:
    """
    Sample task that says goodbye!

    Returns:
        A farewell for your collection
    """
    return "Goodbye, prefect-azure!"
