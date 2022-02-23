from prefect import flow

from prefect_azure.tasks import (
    goodbye_prefect_azure,
    hello_prefect_azure,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_azure)
    print(goodbye_prefect_azure)
