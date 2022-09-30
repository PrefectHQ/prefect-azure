from typing import Tuple
from unittest.mock import MagicMock, Mock

import pytest
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential
from azure.mgmt.containerinstance.operations import ContainerGroupsOperations
from azure.mgmt.resource import ResourceManagementClient

import prefect_azure.container_instance
from prefect_azure import ContainerInstanceCredentials
from prefect_azure.container_instance import (
    ContainerGroupProvisioningState,
    ContainerInstanceJob,
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
def aci_block():
    """
    Returns a basic initialized ACI infrastructure block suitable for use
    in multiple tests.
    """
    client_id = "testclientid"
    client_secret = "testclientsecret"
    tenant_id = "testtenandid"

    credentials = ContainerInstanceCredentials(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )

    aci_block = ContainerInstanceJob(
        command=["test"],
        aci_credentials=credentials,
        azure_resource_group_name="testgroup",
        subscription_id="subid",
    )

    return aci_block


@pytest.fixture()
def mock_aci_client(monkeypatch, mock_resource_client):

    mock_aci_client = Mock()
    mock_container_groups = Mock()
    mock_aci_client.container_groups.return_value = mock_container_groups
    monkeypatch.setattr(
        ContainerInstanceJob, "_create_aci_client", Mock(return_value=mock_aci_client)
    )

    return mock_aci_client


@pytest.fixture()
def mock_container_groups(monkeypatch):
    container_groups = MagicMock(spec=ContainerGroupsOperations)
    monkeypatch.setattr(
        "azure.mgmt.containerinstance.operations.ContainerGroupsOperations",
        MagicMock(return_value=container_groups),
    )

    return container_groups


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
def test_credentials_are_used(aci_block: ContainerInstanceJob, monkeypatch):
    credentials = aci_block.aci_credentials
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

    aci_block.run()

    mock_client_id.assert_called_once()
    mock_client_secret.assert_called_once()
    mock_tenant_id.assert_called_once()
    mock_credential.assert_called_once_with(
        client_id=client_id, client_secret=client_secret, tenant_id=tenant_id
    )


def test_container_creation_call(mock_aci_client, aci_block):
    # ensure the block always tries to call the Azure SDK to create the container
    aci_block.run()
    mock_aci_client.container_groups.begin_create_or_update.assert_called_once()


def test_delete_after_group_creation_failure(aci_block, mock_aci_client, monkeypatch):
    # if provisioning failed, the container group should be deleted
    mock_container_group = Mock()
    mock_container_group.provisioning_state.return_value = (
        ContainerGroupProvisioningState.FAILED
    )
    monkeypatch.setattr(
        aci_block, "_wait_for_task_container_start", mock_container_group
    )
    aci_block.run()
    mock_aci_client.container_groups.begin_delete.assert_called_once()


def test_delete_after_group_creation_success(aci_block, mock_aci_client, monkeypatch):
    # if provisioning was successful, the container group should eventually be deleted
    mock_container_group = Mock()
    mock_container_group.provisioning_state.return_value = (
        ContainerGroupProvisioningState.SUCCEEDED
    )
    monkeypatch.setattr(
        aci_block, "_wait_for_task_container_start", mock_container_group
    )
    aci_block.run()
    mock_aci_client.container_groups.begin_delete.assert_called_once()


def test_delete_after_after_exception(aci_block, mock_aci_client, monkeypatch):
    # if an exception was thrown while waiting for container group provisioning,
    # the group should be deleted
    mock_aci_client.container_groups.begin_create_or_update.side_effect = (
        HttpResponseError(message="it broke")
    )

    with pytest.raises(HttpResponseError):
        aci_block.run()
        mock_aci_client.container_groups.begin_create_or_update.assert_called_once()
        mock_aci_client.container_groups.begin_delete.assert_called_once()
