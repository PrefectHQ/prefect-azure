import json
from typing import Tuple, Union
from unittest.mock import MagicMock, Mock

import dateutil.parser
import pytest
from anyio.abc import TaskStatus
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential
from azure.mgmt.resource import ResourceManagementClient
from pydantic import SecretStr

import prefect_azure.container_instance
from prefect_azure import ContainerInstanceCredentials
from prefect_azure.container_instance import (
    ContainerGroupProvisioningState,
    ContainerInstanceJob,
    ContainerInstanceJobResult,
)

# Helper functions


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


def create_mock_container_group(state: str, exit_code: Union[int, None]):
    """
    Creates a mock container group with a single container to serve as a stand-in for
    an Azure ContainerInstanceManagementClient's container_group property.

    Args:
        state: The state the single container in the group should report.
        exit_code: The container's exit code, or None

    Returns:
        A mock container group.
    """
    container_group = Mock()
    containers = MagicMock()
    container = Mock()
    container.instance_view.current_state.state = state
    container.instance_view.current_state.exit_code = exit_code
    containers.__getitem__.return_value = container
    container_group.containers = containers
    return container_group


# Fixtures


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
        resource_group_name="testgroup",
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


# Tests


def test_empty_list_command_validation(container_instance_block):
    # ensure that the default command is set automatically if the user
    # provides an empty command list
    aci_flow_run = ContainerInstanceJob(
        command=[],
        subscription_id=SecretStr("test"),
        resource_group_name="test",
    )
    assert aci_flow_run.command == aci_flow_run._base_flow_run_command()


def test_missing_command_validation():
    # ensure that the default command is set automatically if the user
    # provides None
    aci_flow_run = ContainerInstanceJob(
        command=None,
        subscription_id=SecretStr("test"),
        resource_group_name="test",
    )
    assert aci_flow_run.command == aci_flow_run._base_flow_run_command()


def test_valid_command_validation():
    # ensure the validator allows valid commands to pass through
    command = ["command", "arg1", "arg2"]
    aci_flow_run = ContainerInstanceJob(
        command=command,
        subscription_id=SecretStr("test"),
        resource_group_name="test",
    )
    assert aci_flow_run.command == command


def test_invalid_command_validation():
    # ensure invalid commands cause a validation error
    with pytest.raises(ValueError):
        ContainerInstanceJob(
            command="invalid_command -a",
            subscription_id=SecretStr("test"),
            resource_group_name="test",
        )


def test_container_client_creation(container_instance_block, monkeypatch):
    # verify that the Azure Container Instances client and Azure Resource clients
    # are created correctly.

    mock_azure_credential = Mock(spec=ClientSecretCredential)
    monkeypatch.setattr(
        container_instance_block,
        "_create_credential",
        Mock(return_value=mock_azure_credential),
    )

    # don't use the mock_aci_client or mock_resource_client_fixtures, because we want to
    # test the call to the client constructors to ensure the block is calling them
    # with the correct information.
    mock_container_client_constructor = Mock()
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "ContainerInstanceManagementClient",
        mock_container_client_constructor,
    )

    mock_resource_client_constructor = Mock()
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "ResourceManagementClient",
        mock_resource_client_constructor,
    )

    subscription_id = "test_subscription"
    container_instance_block.subscription_id = SecretStr(value=subscription_id)
    container_instance_block.run()

    mock_resource_client_constructor.assert_called_once_with(
        credential=mock_azure_credential,
        subscription_id=subscription_id,
    )
    mock_container_client_constructor.assert_called_once_with(
        credential=mock_azure_credential,
        subscription_id=subscription_id,
    )


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


def test_entrypoint_used_if_provided(
    container_instance_block, mock_aci_client, monkeypatch
):
    mock_container = Mock()
    mock_container.name = "TestContainer"
    mock_container_constructor = Mock(return_value=mock_container)
    monkeypatch.setattr(
        prefect_azure.container_instance, "Container", mock_container_constructor
    )

    entrypoint = "/test/entrypoint.sh"
    container_instance_block.entrypoint = entrypoint
    container_instance_block.run()

    mock_container_constructor.assert_called_once()
    (_, kwargs) = mock_container_constructor.call_args
    assert kwargs.get("command")[0] == entrypoint


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


@pytest.mark.usefixtures("mock_aci_client")
def test_subnets_included_when_present(container_instance_block, monkeypatch):
    mock_container_group_cls = MagicMock()

    monkeypatch.setattr(
        prefect_azure.container_instance, "ContainerGroup", mock_container_group_cls
    )
    subnet_ids = ["subnet1", "subnet2", "subnet3"]
    container_instance_block.subnet_ids = subnet_ids
    container_instance_block.run()
    mock_container_group_cls.assert_called_once()

    (_, kwargs) = mock_container_group_cls.call_args
    subnet_args = kwargs.get("subnet_ids")
    assert len(subnet_args) == len(subnet_ids)
    for subnet_id in subnet_ids:
        assert subnet_id in subnet_ids


def test_preview():
    # ensures the preview generates the JSON expected
    block_args = {
        "resource_group_name": "test-group",
        "memory": 1.0,
        "cpu": 1.0,
        "gpu_count": 1,
        "gpu_sku": "V100",
        "env": {"FAVORITE_ANIMAL": "cat"},
    }

    block = ContainerInstanceJob(**block_args, subscription_id=SecretStr("test"))

    preview = json.loads(block.preview())

    for (k, v) in block_args.items():
        if k == "env":
            for (k2, v2) in block_args["env"].items():
                assert preview[k][k2] == block_args["env"][k2]

        else:
            assert preview[k] == block_args[k]


def test_output_streaming(
    container_instance_block,
    mock_aci_client,
    mock_running_container_group,
    mock_successful_container_group,
    monkeypatch,
):
    # override datetime.now to ensure run start time is before log line timestamps
    run_start_time = dateutil.parser.parse("2022-10-03T20:40:05.3119525Z")
    mock_datetime = Mock()
    mock_datetime.datetime.now.return_value = run_start_time

    monkeypatch.setattr(prefect_azure.container_instance, "datetime", mock_datetime)

    log_lines = """
2022-10-03T20:41:05.3119525Z 20:41:05.307 | INFO    | Flow run 'ultramarine-dugong' - Created task run "Test-39fdf8ff-0" for task "ACI Test"
2022-10-03T20:41:05.3120697Z 20:41:05.308 | INFO    | Flow run 'ultramarine-dugong' - Executing "Test-39fdf8ff-0" immediately...
2022-10-03T20:41:05.6215928Z 20:41:05.616 | INFO    | Task run "Test-39fdf8ff-0" - Test Message
2022-10-03T20:41:05.7758864Z 20:41:05.775 | INFO    | Task run "Test-39fdf8ff-0" - Finished in state Completed()
"""  # noqa

    # include some overlap in the second batch so we can make sure output
    # is not duplicated
    next_log_lines = """
2022-10-03T20:41:05.6215928Z 20:41:05.616 | INFO    | Task run "Test-39fdf8ff-0" - Test Message
2022-10-03T20:41:05.7758864Z 20:41:05.775 | INFO    | Task run "Test-39fdf8ff-0" - Finished in state Completed()
2022-10-03T20:41:13.0149593Z 20:41:13.012 | INFO    | Flow run 'ultramarine-dugong' - Created task run "Test-39fdf8ff-1" for task "ACI Test"
2022-10-03T20:41:13.0152433Z 20:41:13.013 | INFO    | Flow run 'ultramarine-dugong' - Executing "Test-39fdf8ff-1" immediately...
2022-broken-03T20:41:13.0152433Z 20:41:13.013 | INFO    | Log line with broken timestamp should not be printed
    """  # noqa

    log_count = 0

    def get_logs(*args, **kwargs):
        nonlocal log_count
        logs = Mock()
        if log_count == 0:
            log_count += 1
            logs.content = log_lines
        else:
            logs.content = next_log_lines

        return logs

    run_count = 0

    def get_container_group(**kwargs):
        nonlocal run_count
        run_count += 1
        if run_count < 5:
            return mock_running_container_group
        else:
            return mock_successful_container_group

    mock_aci_client.container_groups.get.side_effect = get_container_group

    mock_log_call = Mock(side_effect=get_logs)
    monkeypatch.setattr(mock_aci_client.containers, "list_logs", mock_log_call)

    mock_write_call = Mock()
    monkeypatch.setattr(container_instance_block, "_write_output_line", mock_write_call)

    monkeypatch.setattr(
        container_instance_block, "_provisioning_succeeded", Mock(return_value=True)
    )

    monkeypatch.setattr(
        container_instance_block,
        "_wait_for_task_container_start",
        Mock(return_value=mock_running_container_group),
    )

    container_instance_block.stream_output = True
    container_instance_block.task_watch_poll_interval = 0.02
    container_instance_block.run()

    # 6 lines should be written because of the nine test log lines, two overlap
    # and should not be written twice, and one has a broken timestamp so should
    # not be written
    assert mock_write_call.call_count == 6
