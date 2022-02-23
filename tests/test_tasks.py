from prefect import flow

from prefect_azure.tasks import (
    goodbye_prefect_azure,
    hello_prefect_azure,
)


def test_hello_prefect_azure():
    @flow
    def test_flow():
        return hello_prefect_azure()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Hello, prefect-azure!"


def goodbye_hello_prefect_azure():
    @flow
    def test_flow():
        return goodbye_prefect_azure()

    flow_state = test_flow()
    task_state = flow_state.result()
    assert task_state.result() == "Goodbye, prefect-azure!"
