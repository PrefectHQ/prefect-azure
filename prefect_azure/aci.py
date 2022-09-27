import time
import uuid
from typing import Dict, List, Optional

from anyio.abc import TaskStatus
from azure.core.credentials import TokenCredential
from azure.core.polling import LROPoller
from azure.identity import ClientSecretCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    EnvironmentVariable,
    GpuResource,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
    ContainerState,
)
from azure.mgmt.resource import ResourceManagementClient

from prefect.docker import get_prefect_image_name
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.infrastructure.docker import DockerRegistry
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field, SecretStr
from typing_extensions import Literal

from .credentials import ACICredentials

ACI_DEFAULT_CPU = 1.0
ACI_DEFAULT_MEMORY = 1.0
ACI_DEFAULT_GPU = 0.0


class ACITaskResult(InfrastructureResult):
    pass


# TODO: Consider renaming. This class was modeled after ECSTask, but 'Task' has actual meaning on ECS.
# Using it here might be confusing since this is a flow runner, not a task runner. Perhaps `ACIFlowRunner`
# or something similar?
class ACITask(Infrastructure):
    """
    <span class="badge-api experimental"/>
    Run a command as an Azure Container Instances task.
    Note this block is experimental. The interface may change without notice.
    """

    _block_type_slug = "aci-task"
    _block_type_name = "Azure Container Instances Task"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/6AiQ6HRIft8TspZH7AfyZg/39fd82bdbb186db85560f688746c8cdd/azure.png?h=250"  # noqa
    _description = "Run tasks using Azure Container Instances. Note this block is experimental. The interface may change without notice."  # noqa

    type: Literal["aci-task"] = Field(
        default="aci-task", description="The slug for this task type."
    )
    azure_credentials: ACICredentials = None
    azure_resource_group_name: str = Field(
        title="Azure Resource Group Name",
        default=None,
        description=(
            "The name of the Azure Resource Group in which to run Prefect ACI tasks."
        ),
    )
    subscription_id: SecretStr = Field(
        title="Azure Subscription ID",
        default=None,
        description="The ID of the Azure subscription to create containers under.",
    )
    image: Optional[str] = Field(
        default_factory=get_prefect_image_name,
        description=(
            "The image to use for the Prefect container in the task. This value "
            "defaults to a Prefect base image matching your local versions."
        ),
    )
    entrypoint: str = Field(
        default="/opt/prefect/entrypoint.sh",
        description=(
            "The entrypoint of the container you wish you run. This value defaults to the "
            "entrypoint used by Prefect images and should only be changed when using a custom "
            "image that is not based on an official Prefect image. Any commands set on deployments "
            "will be passed to the entrypoint as parameters."
        ),
    )
    image_registry: Optional[DockerRegistry] = None
    cpu: float = Field(
        title="CPU",
        default=ACI_DEFAULT_CPU,
        description=(
            "The number of virtual CPUs to assign to the task container. "
            f"If not provided, a default value of {ACI_DEFAULT_CPU} will be used."
        ),
    )
    gpu_count: Optional[float] = Field(
        title="GPU Count",
        default=None,
        description=(
            "The number of GPUs to assign to the task container. "
            f"If not provided, no GPU will be used."
        ),
    )
    gpu_sku: Optional[str] = Field(
        title="GPU SKU",
        default=None,
        description=(
            "The Azure GPU SKU to use. See the ACI documentation for a list of "
            "GPU SKUs available in each Azure region."
        ),
    )
    memory: float = Field(
        default=ACI_DEFAULT_MEMORY,
        description=(
            "The amount of memory in gigabytes to provide to the ACI task. Valid amounts are "
            "specified in the Azure documentation. If not provided, a default value of "
            f"{ACI_DEFAULT_MEMORY} will be used unless present on the task definition."
        ),
    )
    stream_output: bool = Field(
        default=None,
        description=(
            "If `True`, logs will be streamed from the Prefect container to the local "
            "console. Unless you have configured AWS CloudWatch logs manually on your "
            "task definition, this requires the same prerequisites outlined in "
            "`configure_cloudwatch_logs`."
        ),
    )
    env: Dict[str, Optional[str]] = Field(
        title="Environment Variables",
        default_factory=dict,
        description=(
            "Environment variables to provide to the task run. These variables are set "
            "on the Prefect container at task runtime. These will not be set on the "
            "task definition."
        ),
    )
    # Execution settings
    task_start_timeout_seconds: int = Field(
        default=240,
        description=(
            "The amount of time to watch for the start of the ACI task "
            "before marking it as failed. The task must enter a RUNNING state to be "
            "considered started."
        ),
    )
    task_watch_poll_interval: float = Field(
        default=5.0,
        description=(
            "The amount of time to wait between Azure API calls while monitoring the "
            "state of an Azure Container Instances task."
        ),
    )

    @sync_compatible
    async def run(self, task_status: Optional[TaskStatus] = None) -> ACITaskResult:
        """
        Run the configured task on ACI.
        """
        if not self.command:
            raise ValueError("Container cannot be run with empty command.")

        # TODO: determine how to make DefaultAzureCredential work as expected
        # if self.azure_credentials:
        #     self.azure_credentials.login()
        # token_credential = DefaultAzureCredential
        token_credential = ClientSecretCredential(
            tenant_id=self.azure_credentials.tenant_id.get_secret_value(),
            client_id=self.azure_credentials.client_id.get_secret_value(),
            client_secret=self.azure_credentials.client_secret.get_secret_value(),
        )
        aci_client = ContainerInstanceManagementClient(
            credential=token_credential,
            subscription_id=self.subscription_id.get_secret_value(),
        )
        container = self._configure_container(token_credential)
        container_group = self._configure_container_group(token_credential, container)
        created_container_group = None

        try:
            # Create the container group and wait for it to start
            creation_status_poller = aci_client.container_groups.begin_create_or_update(
                self.azure_resource_group_name, container.name, container_group
            )
            created_container_group = await run_sync_in_worker_thread(
                self._wait_for_task_container_start, creation_status_poller
            )

            # If creation succeeded, group provisioning state should be 'Succeeded'
            # and the group should have a single container
            if (
                created_container_group.provisioning_state == "Succeeded"
                and len(created_container_group.containers) == 1
            ):
                if task_status:
                    task_status.started(value=container.name)
                status_code = await run_sync_in_worker_thread(
                    self._watch_task_and_get_exit_code,
                    aci_client,
                    created_container_group,
                )
            else:
                status_code = -1

        finally:
            if created_container_group:
                aci_client.container_groups.begin_delete(
                    resource_group_name=self.azure_resource_group_name,
                    container_group_name=created_container_group.name,
                )

        return ACITaskResult(identifier=container.name, status_code=status_code)

    def preview(self) -> str:
        return ""

    def _configure_container(self, credential: TokenCredential) -> Container:
        # setup container environment variables
        environment = [
            EnvironmentVariable(name=k, value=v)
            for (k, v) in {**self._base_environment(), **self.env}.items()
        ]
        # all container names in a resource group must be unique
        container_name = str(uuid.uuid4())
        container_resource_requirements = self._configure_container_resources()

        if self.entrypoint:
            self.command.insert(0, self.entrypoint)

        return Container(
            name=container_name,
            image=self.image,
            command=self.command,
            resources=container_resource_requirements,
            environment_variables=environment,
        )

    def _configure_container_resources(self) -> ResourceRequirements:
        gpu_resource = (
            GpuResource(count=self.gpu_count, sku=self.gpu_sku)
            if self.gpu_count and self.gpu_sku
            else None
        )
        container_resource_requests = ResourceRequests(
            memory_in_gb=self.memory, cpu=self.cpu, gpu=gpu_resource
        )

        return ResourceRequirements(requests=container_resource_requests)

    def _configure_container_group(
        self, credential: TokenCredential, container: Container
    ) -> ContainerGroup:
        # Load the resource group, so we can set the container group location correctly.
        resource_group_client = ResourceManagementClient(
            credential=credential,
            subscription_id=self.subscription_id.get_secret_value(),
        )
        resource_group = resource_group_client.resource_groups.get(
            self.azure_resource_group_name
        )

        image_registry_credential = (
            ImageRegistryCredential(
                server=self.image_registry.registry_url,
                username=self.image_registry.username,
                password=self.image_registry.password,
            )
            if self.image_registry
            else None
        )

        return ContainerGroup(
            location=resource_group.location,
            containers=[container],
            os_type=OperatingSystemTypes.linux,
            restart_policy=ContainerGroupRestartPolicy.never,
            image_registry_credentials=image_registry_credential,
        )

    def _wait_for_task_container_start(
        self, creation_status_poller: LROPoller[ContainerGroup]
    ) -> Optional[ContainerGroup]:
        """
        Wait for the result of group and container creation.
        """
        t0 = time.time()
        timeout = self.task_start_timeout_seconds

        while not creation_status_poller.done():
            elapsed_time = time.time() - t0

            if timeout and elapsed_time > timeout:
                raise RuntimeError(
                    f"Timed out after {elapsed_time}s while watching waiting for container start."
                )
            time.sleep(self.task_watch_poll_interval)

        return creation_status_poller.result()

    def _watch_task_and_get_exit_code(
        self, client: ContainerInstanceManagementClient, container_group: ContainerGroup
    ) -> int:
        status_code = -1
        running_container = container_group.containers[0]
        current_state = running_container.instance_view.current_state.state

        # return exit code if flow run already finished:
        if current_state == "Terminated":
            return running_container.instance_view.current_state.exit_code

        # otherwise, watch until it finishes
        while current_state != "Terminated":
            container_group = client.container_groups.get(
                resource_group_name=self.azure_resource_group_name,
                container_group_name=container_group.name,
            )

            container = container_group.containers[0]
            current_state = container.instance_view.current_state.state

            if current_state == "Terminated":
                status_code = container.instance_view.current_state.exit_code
                break

            time.sleep(self.task_watch_poll_interval)

        return status_code

    def _base_aci_flow_run_command(self) -> List[str]:
        """
        Generate a command for a flow run job on ACI.
        """
        return ["/opt/prefect/entrypoint.sh"] + self._base_flow_run_command()
