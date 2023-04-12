import uuid
from typing import Dict, List, Tuple, Union
from unittest.mock import AsyncMock, MagicMock, Mock

import dateutil.parser
import pytest
from anyio.abc import TaskStatus
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import ClientSecretCredential
from azure.mgmt.containerinstance.models import (
    EnvironmentVariable,
    ImageRegistryCredential,
    UserAssignedIdentities,
)
from azure.mgmt.resource import ResourceManagementClient
from prefect.client.schemas import FlowRun
from prefect.exceptions import InfrastructureNotAvailable, InfrastructureNotFound
from prefect.infrastructure.docker import DockerRegistry
from prefect.settings import get_current_settings
from pydantic import SecretStr

import prefect_azure.container_instance
from prefect_azure import AzureContainerInstanceCredentials
from prefect_azure.workers.container_instance import (
    AzureContainerJobConfiguration,
    AzureContainerWorker,
    AzureContainerWorkerResult,
    ContainerGroupProvisioningState,
    ContainerRunState,
)


# Helper functions
def credential_values(
    credentials: AzureContainerInstanceCredentials,
) -> Tuple[str, str, str]:
    """
    Helper function to extract values from an Azure container instances
    credential block

    Args:
        credentials: The credential to extract values from

    Returns:
        A tuple containing (client_id, client_secret, tenant_id) from
        the credentials block
    """
    return (
        credentials.client_id,
        credentials.client_secret.get_secret_value(),
        credentials.tenant_id,
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
    container = Mock()
    container.instance_view.current_state.state = state
    container.instance_view.current_state.exit_code = exit_code
    containers = [container]
    container_group.containers = containers
    # Azure assigns all provisioned container groups a stringified
    # UUID name.
    container_group.name = str(uuid.uuid4())
    return container_group


@pytest.fixture()
def running_worker_container_group():
    """
    A fixture that returns a mock container group simulating a
    a container group that is currently running a flow run.
    """
    container_group = create_mock_container_group(state="Running", exit_code=None)
    container_group.provisioning_state = ContainerGroupProvisioningState.SUCCEEDED
    return container_group


@pytest.fixture()
def completed_worker_container_group():
    """
    A fixture that returns a mock container group simulating a
    a container group that successfully completed its flow run.
    """
    container_group = create_mock_container_group(state="Terminated", exit_code=0)
    container_group.provisioning_state = ContainerGroupProvisioningState.SUCCEEDED

    return container_group


# Fixtures
@pytest.fixture
def aci_credentials(monkeypatch):
    client_id = "test_client_id"
    client_secret = "test_client_secret"
    tenant_id = "test_tenant_id"

    mock_credential = Mock(wraps=ClientSecretCredential, return_value=Mock())

    monkeypatch.setattr(
        prefect_azure.credentials,
        "ClientSecretCredential",
        mock_credential,
    )

    credentials = AzureContainerInstanceCredentials(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )

    return credentials


@pytest.fixture
def aci_worker(monkeypatch):
    mock_prefect_client = Mock()
    mock_prefect_client.read_flow = AsyncMock()
    mock_prefect_client.read_flow.return_value = FlowRun(name="test_flow")
    monkeypatch.setattr(
        prefect_azure.workers.container_instance,
        "get_client",
        Mock(return_value=mock_prefect_client),
    )
    return AzureContainerWorker(work_pool_name="test_pool")


@pytest.fixture()
def job_configuration(aci_credentials, worker_flow_run):
    """
    Returns a basic initialized ACI infrastructure block suitable for use
    in a variety of tests.
    """
    container_instance_configuration = AzureContainerJobConfiguration(
        command="test",
        aci_credentials=aci_credentials,
        resource_group_name="test_group",
        subscription_id=SecretStr("sub_id"),
        name=None,
        task_watch_poll_interval=0.05,
        stream_output=False,
    )

    container_instance_configuration.prepare_for_flow_run(worker_flow_run)
    return container_instance_configuration


@pytest.fixture()
def mock_aci_client(monkeypatch, mock_resource_client):
    """
    A fixture that provides a mock Azure Container Instances client
    """
    container_groups = Mock(name="container_group")
    creation_status_poller = Mock(name="created container groups")
    creation_status_poller_result = Mock(name="created container groups result")
    container_groups.begin_create_or_update.side_effect = (
        lambda *args: creation_status_poller
    )
    creation_status_poller.result.side_effect = lambda: creation_status_poller_result
    creation_status_poller_result.provisioning_state = (
        ContainerGroupProvisioningState.SUCCEEDED
    )
    creation_status_poller_result.name = str(uuid.uuid4())
    container = Mock()
    container.instance_view.current_state.exit_code = 0
    container.instance_view.current_state.state = ContainerRunState.TERMINATED
    containers = Mock(name="containers", containers=[container])
    container_groups.get.side_effect = [containers]
    creation_status_poller_result.containers = [containers]

    aci_client = Mock(container_groups=container_groups)
    monkeypatch.setattr(
        prefect_azure.credentials,
        "ContainerInstanceManagementClient",
        Mock(return_value=aci_client),
    )
    return aci_client


@pytest.fixture()
def mock_resource_client(monkeypatch):
    mock_resource_client = MagicMock(spec=ResourceManagementClient)

    def return_group(name: str):
        client = ResourceManagementClient
        return client.models().ResourceGroup(name=name, location="useast")

    mock_resource_client.resource_groups.get = Mock(side_effect=return_group)

    monkeypatch.setattr(
        AzureContainerInstanceCredentials,
        "get_resource_client",
        MagicMock(return_value=mock_resource_client),
    )

    return mock_resource_client


@pytest.fixture
def worker_flow_run():
    return FlowRun(name="test-flow")


# Tests


def test_worker_valid_command_validation(aci_credentials):
    # ensure the validator allows valid commands to pass through
    command = "command arg1 arg2"

    aci_job_config = AzureContainerJobConfiguration(
        command=command,
        subscription_id=SecretStr("test"),
        resource_group_name="test",
        aci_credentials=aci_credentials,
    )

    assert aci_job_config.command == command


def test_worker_invalid_command_validation(aci_credentials):
    # ensure invalid commands cause a validation error
    with pytest.raises(ValueError):
        AzureContainerJobConfiguration(
            command=["invalid_command", "arg1", "arg2"],  # noqa
            subscription_id=SecretStr("test"),
            resource_group_name="test",
            aci_credentials=aci_credentials,
        )


async def test_worker_container_client_creation(
    aci_worker, worker_flow_run, job_configuration, aci_credentials, monkeypatch
):
    # verify that the Azure Container Instances client and Azure Resource clients
    # are created correctly.

    mock_azure_credential = Mock(spec=ClientSecretCredential)
    monkeypatch.setattr(
        prefect_azure.credentials,
        "ClientSecretCredential",
        Mock(return_value=mock_azure_credential),
    )

    # don't use the mock_aci_client or mock_resource_client_fixtures, because we want to
    # test the call to the client constructors to ensure the block is calling them
    # with the correct information.
    mock_container_client_constructor = Mock()
    monkeypatch.setattr(
        prefect_azure.credentials,
        "ContainerInstanceManagementClient",
        mock_container_client_constructor,
    )

    mock_resource_client_constructor = Mock()
    monkeypatch.setattr(
        prefect_azure.credentials,
        "ResourceManagementClient",
        mock_resource_client_constructor,
    )

    subscription_id = "test_subscription"
    job_configuration.subscription_id = SecretStr(value=subscription_id)
    with pytest.raises(RuntimeError):
        await aci_worker.run(worker_flow_run, job_configuration)

    mock_resource_client_constructor.assert_called_once_with(
        credential=mock_azure_credential,
        subscription_id=subscription_id,
    )
    mock_container_client_constructor.assert_called_once_with(
        credential=mock_azure_credential,
        subscription_id=subscription_id,
    )


@pytest.mark.usefixtures("mock_aci_client")
async def test_worker_credentials_are_used(
    aci_worker,
    worker_flow_run,
    job_configuration,
    aci_credentials,
    mock_aci_client,
    mock_resource_client,
    monkeypatch,
):
    (client_id, client_secret, tenant_id) = credential_values(aci_credentials)

    mock_client_secret = Mock(name="Mock client secret", return_value=client_secret)
    mock_credential = Mock(wraps=ClientSecretCredential, return_value=Mock())

    monkeypatch.setattr(
        aci_credentials.client_secret, "get_secret_value", mock_client_secret
    )
    monkeypatch.setattr(
        prefect_azure.credentials, "ClientSecretCredential", mock_credential
    )
    job_configuration.aci_credentials = aci_credentials

    with pytest.raises(RuntimeError):
        await aci_worker.run(worker_flow_run, job_configuration)

    mock_client_secret.assert_called_once()
    mock_credential.assert_called_once_with(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )


async def test_aci_worker_deployment_call(
    mock_aci_client,
    mock_resource_client,
    completed_worker_container_group,
    aci_worker,
    worker_flow_run,
    job_configuration,
    monkeypatch,
):
    # simulate a successful deployment of a container group to Azure
    monkeypatch.setattr(
        aci_worker,
        "_get_container_group",
        Mock(return_value=completed_worker_container_group),
    )

    mock_poller = Mock()
    # the deployment poller should return a successful deployment
    mock_poller.done = Mock(return_value=True)
    mock_poller_result = MagicMock()
    mock_poller_result.properties.provisioning_state = (
        ContainerGroupProvisioningState.SUCCEEDED
    )
    mock_poller.result = Mock(return_value=mock_poller_result)

    mock_resource_client.deployments.begin_create_or_update = Mock(
        return_value=mock_poller
    )

    # ensure the worker always tries to call the Azure deployments SDK
    # to create the container
    await aci_worker.run(worker_flow_run, job_configuration)
    mock_resource_client.deployments.begin_create_or_update.assert_called_once()


async def test_worker_uses_entrypoint_if_provided(
    aci_worker,
    worker_flow_run,
    job_configuration,
    mock_aci_client,
    mock_resource_client,
    monkeypatch,
):
    mock_deployment_call = Mock()
    mock_resource_client.deployments.begin_create_or_update = mock_deployment_call

    entrypoint = "/test/entrypoint.sh"
    job_configuration.entrypoint = entrypoint

    # We haven't mocked out the container group creation, so this should fail
    # and that's ok. We just want to ensure the entrypoint is used.
    with pytest.raises(RuntimeError):
        await aci_worker.run(worker_flow_run, job_configuration)

    mock_deployment_call.assert_called_once()
    (_, kwargs) = mock_deployment_call.call_args

    deployment_parameters = kwargs.get("parameters").properties
    deployment_arm_template = deployment_parameters.template
    # We're only interested in the first resource, which is the container group
    # we're trying to create.
    deployment_resources = deployment_arm_template["resources"][0]
    called_command: str = kwargs.get("command")
    assert called_command.startswith(entrypoint)


def test_runs_without_entrypoint(
    container_instance_block, mock_aci_client, monkeypatch
):
    mock_container = Mock()
    mock_container.name = "TestContainer"
    mock_container_constructor = Mock(return_value=mock_container)
    monkeypatch.setattr(
        prefect_azure.container_instance, "Container", mock_container_constructor
    )

    default_command = container_instance_block.command
    container_instance_block.entrypoint = None
    container_instance_block.run()

    mock_container_constructor.assert_called_once()
    (_, kwargs) = mock_container_constructor.call_args
    assert kwargs.get("command")[0] == " ".join(default_command)


def test_delete_after_group_creation_failure(
    job_configuration, mock_aci_client, monkeypatch
):
    # if provisioning failed, the container group should be deleted
    mock_container_group = Mock()
    mock_container_group.provisioning_state.return_value = (
        ContainerGroupProvisioningState.FAILED
    )
    container_worker = AzureContainerWorker()

    monkeypatch.setattr(
        container_worker, "_wait_for_task_container_start", mock_container_group
    )
    with pytest.raises(RuntimeError, match="Container creation failed"):
        container_worker.run(FlowRun(name="Test"), configuration=job_configuration)
    mock_aci_client.container_groups.begin_delete.assert_called_once()


def test_no_delete_if_no_container_group(
    container_instance_block, mock_aci_client, monkeypatch
):
    # if provisioning failed because the ACI client returned nothing,
    # we should not attempt to delete the container group because we don't
    # have enough data to try
    monkeypatch.setattr(
        container_instance_block,
        "_wait_for_task_container_start",
        Mock(return_value=None),
    )
    with pytest.raises(RuntimeError, match="Container creation failed"):
        container_instance_block.run()
    mock_aci_client.container_groups.begin_delete.assert_not_called()


def test_delete_after_group_creation_success(
    container_instance_block, mock_aci_client, monkeypatch
):
    # if provisioning was successful, the container group should eventually be deleted
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

    task_status.started.assert_called_once_with(
        value=mock_successful_container_group.name
    )


@pytest.mark.usefixtures("mock_aci_client")
def test_task_status_not_started_on_provisioning_failure(
    container_instance_block, mock_successful_container_group, monkeypatch
):
    monkeypatch.setattr(
        container_instance_block, "_provisioning_succeeded", Mock(return_value=False)
    )

    task_status = Mock(spec=TaskStatus)
    with pytest.raises(RuntimeError, match="Container creation failed"):
        container_instance_block.run(task_status=task_status)
    task_status.started.assert_not_called()


def test_provisioning_timeout_throws_exception(
    mock_aci_client, container_instance_block
):
    mock_poller = Mock()
    mock_poller.done.return_value = False
    mock_aci_client.container_groups.begin_create_or_update.side_effect = (
        lambda *args: mock_poller
    )

    # avoid delaying test runs
    container_instance_block.task_watch_poll_interval = 0.09
    container_instance_block.task_start_timeout_seconds = 0.10

    with pytest.raises(RuntimeError, match="Timed out after"):
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
    assert isinstance(result, AzureContainerWorkerResult)


def test_watch_for_quick_termination(
    mock_aci_client,
    mock_successful_container_group,
    container_instance_block,
    monkeypatch,
):
    # ensure that everything works as expected in the case where the container has
    # already finished its flow run by the time the poller picked up the container
    # group's successful provisioning status.

    monkeypatch.setattr(
        container_instance_block, "_provisioning_succeeded", Mock(return_value=True)
    )

    monkeypatch.setattr(
        container_instance_block,
        "_wait_for_task_container_start",
        Mock(return_value=mock_successful_container_group),
    )

    result = container_instance_block.run()

    # ensure the watcher didn't need to call to check status since the run
    # already completed.
    mock_aci_client.container_groups.get.assert_not_called()
    # ensure the run completed
    assert isinstance(result, AzureContainerWorkerResult)


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


def test_preview(aci_credentials):
    # ensures the preview generates the JSON expected
    # block_args = {
    #     "resource_group_name": "test-group",
    #     "memory": 1.0,
    #     "cpu": 1.0,
    #     "gpu_count": 1,
    #     "gpu_sku": "V100",
    #     "env": {"FAVORITE_ANIMAL": "cat"},
    # }

    raise NotImplementedError("This test needs to be updated to use the new ACI worker")
    # block = AzureContainerInstanceJob(
    #     **block_args,
    #     aci_credentials=aci_credentials,
    #     subscription_id=SecretStr("test")
    # )
    #
    # preview = json.loads(block.preview())
    #
    # for (k, v) in block_args.items():
    #     if k == "env":
    #         for (k2, v2) in block_args["env"].items():
    #             assert preview[k][k2] == block_args["env"][k2]
    #
    #     else:
    #         assert preview[k] == block_args[k]


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
        elif log_count == 1:
            log_count += 1
            logs.content = next_log_lines
        else:
            logs.content = ""

        return logs

    run_count = 0

    def get_container_group(**kwargs):
        nonlocal run_count
        run_count += 1
        if run_count < 4:
            return mock_running_container_group
        else:
            return mock_successful_container_group

    mock_aci_client.container_groups.get.side_effect = get_container_group

    mock_log_call = Mock(side_effect=get_logs)
    monkeypatch.setattr(mock_aci_client.containers, "list_logs", mock_log_call)

    mock_write_call = Mock(wraps=container_instance_block._write_output_line)
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
    container_instance_block.name = "streaming test"
    container_instance_block.task_watch_poll_interval = 0.02
    container_instance_block.run()

    # 6 lines should be written because of the nine test log lines, two overlap
    # and should not be written twice, and one has a broken timestamp so should
    # not be written
    assert mock_write_call.call_count == 6


def test_block_accessible_in_module_toplevel():
    # will raise an exception and fail the test if `AzureContainerInstanceJob`
    # is not accessible directly from `prefect_azure`
    from prefect_azure import AzureContainerInstanceJob  # noqa


def test_registry_credentials(container_instance_block, mock_aci_client, monkeypatch):
    mock_container_group_constructor = MagicMock()

    monkeypatch.setattr(
        prefect_azure.container_instance,
        "ContainerGroup",
        mock_container_group_constructor,
    )

    registry = DockerRegistry(
        username="username",
        password="password",
        registry_url="https://myregistry.dockerhub.com",
    )

    container_instance_block.image_registry = registry
    container_instance_block.run()

    mock_container_group_constructor.assert_called_once()

    (_, kwargs) = mock_container_group_constructor.call_args
    registry_arg: List[ImageRegistryCredential] = kwargs.get(
        "image_registry_credentials"
    )

    # ensure the registry was used, passed as a list the way the Azure SDK expects it,
    # and correctly converted to an Azure ImageRegistryCredential.
    assert registry_arg is not None
    assert isinstance(registry_arg, list)

    registry_object = registry_arg[0]
    assert isinstance(registry_object, ImageRegistryCredential)
    assert registry_object.username == registry.username
    assert registry_object.password == registry.password.get_secret_value()
    assert registry_object.server == registry.registry_url


def test_secure_environment_variables(
    container_instance_block, mock_aci_client, monkeypatch
):
    # setup environment containing an API key we want to keep secret
    base_env: Dict[str, str] = get_current_settings().to_environment_variables(
        exclude_unset=True
    )
    base_env["PREFECT_API_KEY"] = "my-api-key"

    base_env_call = Mock(return_value=base_env)
    monkeypatch.setattr(container_instance_block, "_base_environment", base_env_call)

    mock_container_constructor = Mock()
    monkeypatch.setattr(
        prefect_azure.container_instance, "Container", mock_container_constructor
    )

    container_instance_block.run()

    mock_container_constructor.assert_called_once()
    (_, kwargs) = mock_container_constructor.call_args
    env_variables_parameter: List = kwargs.get("environment_variables")

    api_key_aci_env_variable: List[EnvironmentVariable] = list(
        filter(lambda v: v.name == "PREFECT_API_KEY", env_variables_parameter)
    )

    # ensure the env variable made it into the list of env variables set in the
    # ACI container
    assert len(api_key_aci_env_variable) == 1
    assert api_key_aci_env_variable[0].name == "PREFECT_API_KEY"
    # ensure that `secure_value` was used instead of `value`
    assert api_key_aci_env_variable[0].value is None
    assert api_key_aci_env_variable[0].secure_value == "my-api-key"


async def test_kill_deletes_container_group(
    container_instance_block, mock_aci_client, mock_running_container_group, monkeypatch
):
    container_group_name = "test_container_group"
    mock_running_container_group.name = container_group_name

    # simulate a running container to avoid `InfrastructureNotAvailable` exception
    # since it is covered in a separate test
    container = Mock(name="container")
    container.instance_view.current_state.state = ContainerRunState.RUNNING

    mock_aci_client.container_groups.get.side_effect = Mock(
        name="container_group", return_value=mock_running_container_group
    )

    # shorten wait time
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "CONTAINER_GROUP_DELETION_TIMEOUT_SECONDS",
        0.1,
    )
    container_instance_block.task_watch_poll_interval = 0.02

    await container_instance_block.kill(container_group_name)

    mock_aci_client.container_groups.begin_delete.assert_called_once_with(
        resource_group_name=container_instance_block.resource_group_name,
        container_group_name=container_group_name,
    )


async def test_kill_raises_not_found_when_container_group_gone(
    container_instance_block, mock_aci_client
):
    mock_aci_client.container_groups.get.side_effect = ResourceNotFoundError()

    # ResourceNotFoundError means the container group no longers exists on Azure, so
    # it should always cause an InfrastructureNotFound exception
    with pytest.raises(InfrastructureNotFound):
        await container_instance_block.kill("test_container_group")


async def test_kill_raises_not_available_when_container_terminated(
    container_instance_block, mock_aci_client
):
    # mock_aci_client already contains a mock container group that has finished running,
    # i.e. its status is ContainerRunState.TERMINATED, so it should trigger
    # InfrastructureNotAvailable as-is

    with pytest.raises(InfrastructureNotAvailable):
        await container_instance_block.kill("test_container_group")


async def test_run_exits_when_resource_group_disappears(
    container_instance_block, mock_aci_client, mock_running_container_group
):
    """
    Test to ensure that when a flow run is cancelled, the container group will disappear
    mid-run. Ensure the run method exits gracefully instead of showing an exception
    since the user requested cancellation.
    """

    # make the block wait a few times before the container group disappears
    # to simulate what happens when a flow is cancelled before it completes.
    run_count = 0

    def get_container_group(**kwargs):
        nonlocal run_count
        run_count += 1
        if run_count < 3:
            return mock_running_container_group
        else:
            raise ResourceNotFoundError()

    mock_aci_client.container_groups.get.side_effect = get_container_group

    container_instance_block.task_watch_poll_interval = 0.02

    status_code = await container_instance_block.run()

    # Expect a non-success status code. This ensures that the flow status is set to
    # CRASHED if the container group's absence was not caused by cancellation.
    assert status_code != 0


async def test_identities_used_if_provided(
    container_instance_block, mock_aci_client, monkeypatch
):
    mock_container_group = Mock(name="TestContainerGroup")
    mock_container_group_constructor = Mock(return_value=mock_container_group)
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "ContainerGroup",
        mock_container_group_constructor,
    )

    mock_container_group_identity = Mock()
    mock_identities_constructor = Mock(return_value=mock_container_group_identity)
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "ContainerGroupIdentity",
        mock_identities_constructor,
    )

    identities = [
        "/my/managed_identity/one",
        "/my/managed_identity/two",
    ]

    container_instance_block.identities = identities
    await container_instance_block.run()

    expected_identities_param = {
        identity: UserAssignedIdentities() for identity in identities
    }
    mock_identities_constructor.assert_called_once_with(
        type="UserAssigned", user_assigned_identities=expected_identities_param
    )

    mock_container_group_constructor.assert_called_once()
    (_, kwargs) = mock_container_group_constructor.call_args
    assert kwargs.get("identity") == mock_container_group_identity


async def test_dns_config_used_if_provided(
    container_instance_block, mock_aci_client, monkeypatch
):
    mock_container_group = Mock(name="TestContainerGroup")
    mock_container_group_constructor = Mock(return_value=mock_container_group)
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "ContainerGroup",
        mock_container_group_constructor,
    )

    mock_dns_config = Mock()
    mock_dns_config_constructor = Mock(return_value=mock_dns_config)
    monkeypatch.setattr(
        prefect_azure.container_instance,
        "DnsConfiguration",
        mock_dns_config_constructor,
    )

    dns_servers = ["1.2.3.4", "5.6.7.8", "9.10.11.12"]
    container_instance_block.dns_servers = dns_servers
    await container_instance_block.run()

    mock_dns_config_constructor.assert_called_once_with(name_servers=dns_servers)

    mock_container_group_constructor.assert_called_once()
    (_, kwargs) = mock_container_group_constructor.call_args
    assert kwargs.get("dns_config") == mock_dns_config
