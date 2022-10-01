import json
from typing import Tuple, Union
from unittest.mock import MagicMock, Mock

import pytest
from anyio.abc import TaskStatus
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient

import prefect_azure.container_instance
from prefect_azure import ContainerInstanceCredentials
from prefect_azure.container_instance import (
    ContainerGroupProvisioningState,
    ContainerInstanceJob,
    ContainerInstanceJobResult,
)


def credential_values(
    credentials: ContainerInstanceCredentials,
) -> Tuple[str, str, str]:
    """
    Helper function to extract values from an Azure container instances
    credential block

    Args:
        credential: The credential to extract values from

    Returns:
        A tuple containing (client_id, client_secret, tenant_id) from
        the credentials block
    """
    return (
        credentials.client_id.get_secret_value(),
        credentials.client_secret.get_secret_value(),
        credentials.tenant_id.get_secret_value(),
    )


@pytest.fixture()
def container_instance_block():
    """
    Returns a basic initialized ACI infrastructure block suitable for use
    in a variety of tests.
    """
    client_id = "testclientid"
    client_secret = "testclientsecret"
    tenant_id = "testtenandid"

    credentials = ContainerInstanceCredentials(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )

    container_instance_block = ContainerInstanceJob(
        command=["test"],
        aci_credentials=credentials,
        azure_resource_group_name="testgroup",
        subscription_id="subid",
    )

    return container_instance_block


@pytest.fixture()
def mock_aci_client(monkeypatch, mock_resource_client):
    """
    A fixture that provides a mock Azure Container Instances client
    """
    mock_aci_client = Mock()
    mock_container_groups = Mock()
    mock_aci_client.container_groups.return_value = mock_container_groups
    monkeypatch.setattr(
        ContainerInstanceJob, "_create_aci_client", Mock(return_value=mock_aci_client)
    )

    return mock_aci_client


def create_mock_container_group(state: str, exit_code: Union[int, None]):
    container_group = Mock()
    containers = MagicMock()
    container = Mock()
    container.instance_view.current_state.state = state
    container.instance_view.current_state.exit_code = exit_code
    containers.__getitem__.return_value = container
    container_group.containers = containers
    return container_group


@pytest.fixture()
def mock_successful_container_group():
    """
    A fixture that returns a mock container group that mimics a successfully
    provisioned container group returned from Azure.
    """
    return create_mock_container_group(state="Terminated", exit_code=0)


@pytest.fixture()
def mock_running_container_group():
    return create_mock_container_group(state="Running", exit_code=None)


@pytest.fixture()
def mock_resource_client(monkeypatch):
    mock_resource_client = MagicMock(spec=ResourceManagementClient)

    def return_group(name: str):
        client = ResourceManagementClient
        return client.models().ResourceGroup(name=name, location="useast")

    mock_resource_client.resource_groups.get = Mock(side_effect=return_group)

    monkeypatch.setattr(
        ContainerInstanceJob,
        "_create_resource_client",
        MagicMock(return_value=mock_resource_client),
    )

    return mock_resource_client


def test_empty_list_command_validation():
    # ensure that the default command is set automatically if the user
    # provides an empty command list
    aci_flow_run = ContainerInstanceJob(command=[])
    assert aci_flow_run.command == aci_flow_run._base_flow_run_command()


def test_missing_command_validation():
    # ensure that the default command is set automatically if the user
    # provides None
    aci_flow_run = ContainerInstanceJob(command=None)
    assert aci_flow_run.command == aci_flow_run._base_flow_run_command()


def test_valid_command_validation():
    # ensure the validator allows valid commands to pass through
    command = ["command", "arg1", "arg2"]
    aci_flow_run = ContainerInstanceJob(command=command)
    assert aci_flow_run.command == command


def test_invalid_command_validation():
    # ensure invalid commands cause a validation error
    with pytest.raises(ValueError):
        ContainerInstanceJob(command="invalid_command -a")


@pytest.mark.usefixtures("mock_aci_client")
def test_credentials_are_used(
    container_instance_block: ContainerInstanceJob, monkeypatch
):
    credentials = container_instance_block.aci_credentials
    (client_id, client_secret, tenant_id) = credential_values(credentials)

    mock_client_id = Mock(return_value=client_id)
    mock_client_secret = Mock(return_value=client_secret)
    mock_tenant_id = Mock(return_value=tenant_id)
    mock_credential = Mock(wraps=ClientSecretCredential)

    monkeypatch.setattr(credentials.client_id, "get_secret_value", mock_client_id)
    monkeypatch.setattr(
        credentials.client_secret, "get_secret_value", mock_client_secret
    )
    monkeypatch.setattr(credentials.tenant_id, "get_secret_value", mock_tenant_id)
    monkeypatch.setattr(
        prefect_azure.container_instance, "ClientSecretCredential", mock_credential
    )

    container_instance_block.run()

    mock_client_id.assert_called_once()
    mock_client_secret.assert_called_once()
    mock_tenant_id.assert_called_once()
    mock_credential.assert_called_once_with(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )


def test_container_creation_call(mock_aci_client, container_instance_block):
    # ensure the block always tries to call the Azure SDK to create the container
    container_instance_block.run()
    mock_aci_client.container_groups.begin_create_or_update.assert_called_once()


def test_delete_after_group_creation_failure(
    container_instance_block, mock_aci_client, monkeypatch
):
    # if provisioning failed, the container group should be deleted
    mock_container_group = Mock()
    mock_container_group.provisioning_state.return_value = (
        ContainerGroupProvisioningState.FAILED
    )
    monkeypatch.setattr(
        container_instance_block, "_wait_for_task_container_start", mock_container_group
    )
    container_instance_block.run()
    mock_aci_client.container_groups.begin_delete.assert_called_once()


def test_delete_after_group_creation_success(
    container_instance_block, mock_aci_client, monkeypatch
):
    # if provisioning was successful, the container group should eventually be deleted
    mock_container_group = Mock()
    mock_container_group.provisioning_state.return_value = (
        ContainerGroupProvisioningState.SUCCEEDED
    )
    monkeypatch.setattr(
        container_instance_block, "_wait_for_task_container_start", mock_container_group
    )
    container_instance_block.run()
    mock_aci_client.container_groups.begin_delete.assert_called_once()


def test_delete_after_after_exception(
    container_instance_block, mock_aci_client, monkeypatch
):
    # if an exception was thrown while waiting for container group provisioning,
    # the group should be deleted
    mock_aci_client.container_groups.begin_create_or_update.side_effect = (
        HttpResponseError(message="it broke")
    )

    with pytest.raises(HttpResponseError):
        container_instance_block.run()

    mock_aci_client.container_groups.begin_create_or_update.assert_called_once()


@pytest.mark.usefixtures("mock_aci_client")
def test_task_status_started_on_provisioning_success(
    container_instance_block, mock_successful_container_group, monkeypatch
):
    monkeypatch.setattr(
        container_instance_block, "_provisioning_succeeded", Mock(return_value=True)
    )

    monkeypatch.setattr(
        container_instance_block,
        "_wait_for_task_container_start",
        Mock(return_value=mock_successful_container_group),
    )

    task_status = Mock(spec=TaskStatus)
    container_instance_block.run(task_status=task_status)

    task_status.started.assert_called_once()


@pytest.mark.usefixtures("mock_aci_client")
def test_task_status_not_started_on_provisioning_failure(
    container_instance_block, mock_successful_container_group, monkeypatch
):
    monkeypatch.setattr(
        container_instance_block, "_provisioning_succeeded", Mock(return_value=False)
    )

    task_status = Mock(spec=TaskStatus)
    container_instance_block.run(task_status=task_status)
    task_status.started.assert_not_called()


def test_provisioning_timeout_throws_exception(
    mock_aci_client, container_instance_block
):
    mock_poller = Mock()
    mock_poller.done.return_value = False
    mock_aci_client.container_groups.begin_create_or_update.return_value = mock_poller

    # avoid delaying test runs
    container_instance_block.task_watch_poll_interval = 0.02
    container_instance_block.task_start_timeout_seconds = 0.10

    with pytest.raises(RuntimeError):
        container_instance_block.run()


def test_watch_for_container_termination(
    mock_aci_client,
    mock_running_container_group,
    mock_successful_container_group,
    container_instance_block,
    monkeypatch,
):
    monkeypatch.setattr(
        container_instance_block, "_provisioning_succeeded", Mock(return_value=True)
    )
    monkeypatch.setattr(
        container_instance_block,
        "_wait_for_task_container_start",
        Mock(return_value=mock_running_container_group),
    )

    # make the block wait a few times before we give it a successful result
    # so we can make sure the watcher actually watches instead of skipping
    # the timeout
    run_count = 0

    def get_container_group(**kwargs):
        nonlocal run_count
        run_count += 1
        if run_count < 5:
            return mock_running_container_group
        else:
            return mock_successful_container_group

    mock_aci_client.container_groups.get.side_effect = get_container_group

    container_instance_block.task_watch_poll_interval = 0.02
    result = container_instance_block.run()

    # ensure the watcher was watching
    assert run_count == 5
    assert mock_aci_client.container_groups.get.call_count == run_count
    # ensure the run completed
    assert isinstance(result, ContainerInstanceJobResult)


def test_preview():
    # ensures the preview generates the JSON expected
    block_args = {
        "azure_resource_group_name": "test-group",
        "memory": 1.0,
        "cpu": 1.0,
        "gpu_count": 1,
        "gpu_sku": "V100",
        "env": {"FAVORITE_ANIMAL": "cat"},
    }

    block = ContainerInstanceJob(**block_args)

    preview = json.loads(block.preview())

    for (k, v) in block_args.items():
        if k == "env":
            for (k2, v2) in block_args["env"].items():
                assert preview[k][k2] == block_args["env"][k2]

        else:
            assert preview[k] == block_args[k]
