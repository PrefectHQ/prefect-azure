"""
<span class="badge-api experimental"/>
Integrations with the Azure Container Instances service.
Note this module is experimental. The interfaces within may change without notice.

The `AzureContainerInstanceJob` infrastructure block in this module is ideally
configured via the Prefect UI and run via a Prefect agent, but it can be called directly
as demonstrated in the following examples.

Examples:
    Run a command using an Azure Container Instances container.
    ```python
    AzureContainerInstanceJob(command=["echo", "hello world"]).run()
    ```

    Run a command and stream the container's output to the local terminal.
    ```python
    AzureContainerInstanceJob(
        command=["echo", "hello world"],
        stream_output=True,
    )
    ```

    Run a command with a specific image
    ```python
    AzureContainerInstanceJob(command=["echo", "hello world"], image="alpine:latest")
    ```

    Run a task with custom memory and CPU requirements
    ```python
    AzureContainerInstanceJob(command=["echo", "hello world"], memory=1.0, cpu=1.0)
    ```

    Run a task with custom memory and CPU requirements
    ```python
    AzureContainerInstanceJob(command=["echo", "hello world"], memory=1.0, cpu=1.0)
    ```

    Run a task with custom memory, CPU, and GPU requirements
    ```python
    AzureContainerInstanceJob(command=["echo", "hello world"], memory=1.0, cpu=1.0,
    gpu_count=1, gpu_sku="V100")
    ```

    Run a task with custom environment variables
    ```python
    AzureContainerInstanceJob(
        command=["echo", "hello $PLANET"],
        env={"PLANET": "earth"}
    )
    ```
"""
import datetime
import json
import sys
import time
import uuid
from enum import Enum
from typing import Dict, List, Optional

import dateutil.parser
import prefect.infrastructure.docker
from anyio.abc import TaskStatus
from azure.core.polling import LROPoller
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (
    Container,
    ContainerGroup,
    ContainerGroupRestartPolicy,
    ContainerGroupSubnetId,
    EnvironmentVariable,
    GpuResource,
    ImageRegistryCredential,
    OperatingSystemTypes,
    ResourceRequests,
    ResourceRequirements,
)
from prefect.docker import get_prefect_image_name
from prefect.infrastructure.base import Infrastructure, InfrastructureResult
from prefect.utilities.asyncutils import run_sync_in_worker_thread, sync_compatible
from pydantic import Field, SecretStr
from typing_extensions import Literal

from prefect_azure.credentials import AzureContainerInstanceCredentials

ACI_DEFAULT_CPU = 1.0
ACI_DEFAULT_MEMORY = 1.0
ACI_DEFAULT_GPU = 0.0
DEFAULT_CONTAINER_ENTRYPOINT = "/opt/prefect/entrypoint.sh"


class ContainerGroupProvisioningState(str, Enum):
    """
    Terminal provisioning states for ACI container groups. Per the Azure docs,
    the states in this Enum are the only ones that can be relied on as dependencies.
    """

    SUCCEEDED = "Succeeded"
    FAILED = "Failed"


class ContainerRunState(str, Enum):
    """
    Terminal run states for ACI containers.
    """

    RUNNING = "Running"
    TERMINATED = "Terminated"


class AzureContainerInstanceJobResult(InfrastructureResult):
    """
    The result of an `AzureContainerInstanceJob` run.
    """


class AzureContainerInstanceJob(Infrastructure):
    """
    <span class="badge-api experimental"/>
    Run a command using a container on Azure Container Instances.
    Note this block is experimental. The interface may change without notice.
    """

    _block_type_name = "Azure Container Instance Job"
    _logo_url = "https://images.ctfassets.net/gm98wzqotmnx/6AiQ6HRIft8TspZH7AfyZg/39fd82bdbb186db85560f688746c8cdd/azure.png?h=250"  # noqa
    _description = "Run tasks using Azure Container Instances. Note this block is experimental. The interface may change without notice."  # noqa

    type: Literal["container-instance-job"] = Field(
        default="container-instance-job", description="The slug for this task type."
    )
    aci_credentials: AzureContainerInstanceCredentials
    resource_group_name: str = Field(
        default=...,
        title="Azure Resource Group Name",
        description=(
            "The name of the Azure Resource Group in which to run Prefect ACI tasks."
        ),
    )
    subscription_id: SecretStr = Field(
        default=...,
        title="Azure Subscription ID",
        description="The ID of the Azure subscription to create containers under.",
    )
    image: Optional[str] = Field(
        default_factory=get_prefect_image_name,
        description=(
            "The image to use for the Prefect container in the task. This value "
            "defaults to a Prefect base image matching your local versions."
        ),
    )
    entrypoint: Optional[str] = Field(
        default=DEFAULT_CONTAINER_ENTRYPOINT,
        description=(
            "The entrypoint of the container you wish you run. This value "
            "defaults to the entrypoint used by Prefect images and should only be "
            "changed when using a custom image that is not based on an official "
            "Prefect image. Any commands set on deployments will be passed "
            "to the entrypoint as parameters."
        ),
    )
    image_registry: Optional[prefect.infrastructure.docker.DockerRegistry] = None
    cpu: float = Field(
        title="CPU",
        default=ACI_DEFAULT_CPU,
        description=(
            "The number of virtual CPUs to assign to the task container. "
            f"If not provided, a default value of {ACI_DEFAULT_CPU} will be used."
        ),
    )
    gpu_count: Optional[int] = Field(
        title="GPU Count",
        default=None,
        description=(
            "The number of GPUs to assign to the task container. "
            "If not provided, no GPU will be used."
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
            "The amount of memory in gigabytes to provide to the ACI task. Valid "
            "amounts are specified in the Azure documentation. If not provided, a "
            f"default value of  {ACI_DEFAULT_MEMORY} will be used unless present "
            "on the task definition."
        ),
    )
    subnet_ids: Optional[List[str]] = Field(
        default=None,
        title="Subnet IDs",
        description="A list of Azure subnet IDs the container should be connected to.",
    )
    stream_output: Optional[bool] = Field(
        default=None,
        description=(
            "If `True`, logs will be streamed from the Prefect container to the local "
            "console."
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
            "The amount of time to watch for the start of the ACI container. "
            "before marking it as failed."
        ),
    )
    task_watch_poll_interval: float = Field(
        default=5.0,
        description=(
            "The number of seconds to wait between Azure API calls while monitoring "
            "the state of an Azure Container Instances task."
        ),
    )

    @sync_compatible
    async def run(
        self, task_status: Optional[TaskStatus] = None
    ) -> AzureContainerInstanceJobResult:
        """
        Runs the configured task using an ACI container.

        Args:
            task_status: An optional `TaskStatus` to update when the container starts.

        Returns:
            An `AzureContainerInstanceJobResult` with the container's exit code.
        """

        run_start_time = datetime.datetime.now(datetime.timezone.utc)

        container = self._configure_container()
        container_group = self._configure_container_group(container)
        created_container_group = None

        aci_client = self.aci_credentials.get_container_client(
            self.subscription_id.get_secret_value()
        )

        self.logger.info(
            f"{self._log_prefix}: Preparing to run command {' '.join(self.command)!r} "
            f"in container {container.name!r} ({self.image})..."
        )
        try:
            self.logger.info(f"{self._log_prefix}: Waiting for container creation...")
            # Create the container group and wait for it to start
            creation_status_poller = await run_sync_in_worker_thread(
                aci_client.container_groups.begin_create_or_update,
                self.resource_group_name,
                container.name,
                container_group,
            )
            created_container_group = await run_sync_in_worker_thread(
                self._wait_for_task_container_start, creation_status_poller
            )

            # If creation succeeded, group provisioning state should be 'Succeeded'
            # and the group should have a single container
            if self._provisioning_succeeded(created_container_group):
                self.logger.info(f"{self._log_prefix}: Running command...")
                if task_status:
                    task_status.started(value=created_container_group.name)
                status_code = await run_sync_in_worker_thread(
                    self._watch_task_and_get_exit_code,
                    aci_client,
                    created_container_group,
                    run_start_time,
                )
                self.logger.info(f"{self._log_prefix}: Completed command run.")
            else:
                raise RuntimeError(f"{self._log_prefix}: Container creation failed.")

        finally:
            if created_container_group:
                self.logger.info(f"{self._log_prefix}: Deleting container...")
                await run_sync_in_worker_thread(
                    aci_client.container_groups.begin_delete,
                    resource_group_name=self.resource_group_name,
                    container_group_name=created_container_group.name,
                )

        return AzureContainerInstanceJobResult(
            identifier=created_container_group.name, status_code=status_code
        )

    def preview(self) -> str:
        """
        Provides a summary of how the container will be created when `run` is called.

        Returns:
           A string containing the summary.
        """
        preview = {
            "container_name": "<generated when run>",
            "resource_group_name": self.resource_group_name,
            "memory": self.memory,
            "cpu": self.cpu,
            "gpu_count": self.gpu_count,
            "gpu_sku": self.gpu_sku,
            "env": self._get_environment(),
        }

        return json.dumps(preview)

    def _configure_container(self) -> Container:
        """
        Configures an Azure `Container` using data from the block's fields.

        Returns:
            An instance of `Container` ready to submit to Azure.
        """

        # setup container environment variables
        environment = [
            EnvironmentVariable(name=k, value=v)
            for (k, v) in self._get_environment().items()
        ]
        # all container names in a resource group must be unique
        container_name = str(uuid.uuid4())
        container_resource_requirements = self._configure_container_resources()

        # add the entrypoint if provided, because creating an ACI container with a
        # command overrides the container's built-in entrypoint.
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
        """
        Configures the container's memory, CPU, and GPU resources.

        Returns:
            A `ResourceRequirements` instance initialized with data from this
            `AzureContainerInstanceJob` block.
        """

        gpu_resource = (
            GpuResource(count=self.gpu_count, sku=self.gpu_sku)
            if self.gpu_count and self.gpu_sku
            else None
        )
        container_resource_requests = ResourceRequests(
            memory_in_gb=self.memory, cpu=self.cpu, gpu=gpu_resource
        )

        return ResourceRequirements(requests=container_resource_requests)

    def _configure_container_group(self, container: Container) -> ContainerGroup:
        """
        Configures the container group needed to start a container on ACI.

        Args:
            container: An initialized instance of `Container`.

        Returns:
            An initialized `ContainerGroup` ready to submit to Azure.
        """

        # Load the resource group, so we can set the container group location
        # correctly.

        resource_group_client = self.aci_credentials.get_resource_client(
            self.subscription_id.get_secret_value()
        )

        resource_group = resource_group_client.resource_groups.get(
            self.resource_group_name
        )

        image_registry_credential = (
            ImageRegistryCredential(
                server=self.image_registry.registry_url,
                username=self.image_registry.username,
                password=self.image_registry.password.get_secret_value(),
            )
            if self.image_registry
            else None
        )

        subnet_ids = (
            [ContainerGroupSubnetId(id=subnet_id) for subnet_id in self.subnet_ids]
            if self.subnet_ids
            else None
        )

        return ContainerGroup(
            location=resource_group.location,
            containers=[container],
            os_type=OperatingSystemTypes.linux,
            restart_policy=ContainerGroupRestartPolicy.never,
            image_registry_credentials=image_registry_credential,
            subnet_ids=subnet_ids,
        )

    def _wait_for_task_container_start(
        self, creation_status_poller: LROPoller[ContainerGroup]
    ) -> ContainerGroup:
        """
        Wait for the result of group and container creation.

        Args:
            creation_status_poller: Poller returned by the Azure SDK.

        Raises:
            RuntimeError: Raised if the timeout limit is exceeded before the
            container starts.

        Returns:
            A `ContainerGroup` representing the current status of the group being
            watched.
        """

        t0 = time.time()
        timeout = self.task_start_timeout_seconds

        while not creation_status_poller.done():
            elapsed_time = time.time() - t0

            if timeout and elapsed_time > timeout:
                raise RuntimeError(
                    (
                        f"Timed out after {elapsed_time}s while watching waiting for "
                        "container start."
                    )
                )
            time.sleep(self.task_watch_poll_interval)

        return creation_status_poller.result()

    def _watch_task_and_get_exit_code(
        self,
        client: ContainerInstanceManagementClient,
        container_group: ContainerGroup,
        run_start_time: datetime.datetime,
    ) -> int:
        """
        Waits until the container finishes running and obtains its exit code.

        Args:
            client: An initialized Azure `ContainerInstanceManagementClient`
            container_group: The `ContainerGroup` in which the container resides.

        Returns:
            An `int` representing the container's exit code.
        """

        status_code = -1
        running_container = self._get_container(container_group)
        current_state = running_container.instance_view.current_state.state

        # get any logs the container has already generated
        last_log_time = run_start_time
        if self.stream_output:
            last_log_time = self._get_and_stream_output(
                client, container_group, last_log_time
            )

        # set exit code if flow run already finished:
        if current_state == ContainerRunState.TERMINATED:
            status_code = running_container.instance_view.current_state.exit_code

        while current_state != ContainerRunState.TERMINATED:
            container_group = client.container_groups.get(
                resource_group_name=self.resource_group_name,
                container_group_name=container_group.name,
            )

            container = self._get_container(container_group)
            current_state = container.instance_view.current_state.state

            if self.stream_output:
                last_log_time = self._get_and_stream_output(
                    client, container_group, last_log_time
                )

            if current_state == ContainerRunState.TERMINATED:
                status_code = container.instance_view.current_state.exit_code

            time.sleep(self.task_watch_poll_interval)

        return status_code

    def _get_container(self, container_group: ContainerGroup) -> Container:
        """
        Extracts the job container from a container group.
        """
        return container_group.containers[0]

    def _get_and_stream_output(
        self,
        client: ContainerInstanceManagementClient,
        container_group: ContainerGroup,
        last_log_time: datetime.datetime,
    ) -> datetime.datetime:
        """
        Fetches logs output from the job container and writes all entries after
        a given time to stderr.

        Args:
            client: An initialized `ContainerInstanceManagementClient`
            container_group: The container group that holds the job container.
            last_log_time: The timestamp of the last output line already streamed.

        Returns:
            The time of the most recent output line written by this call.
        """
        logs = self._get_logs(client, container_group)
        return self._stream_output(logs, last_log_time)

    def _get_logs(
        self,
        client: ContainerInstanceManagementClient,
        container_group: ContainerGroup,
        max_lines: int = 100,
    ) -> str:
        """
        Gets the most container logs up to a given maximum.

        Args:
            client: An initialized `ContainerInstanceManagementClient`
            container_group: The container group that holds the job container.
            max_lines: The number of log lines to pull. Defaults to 100.

        Returns:
            A string containing the requested log entries, one per line.
        """
        container = self._get_container(container_group)

        logs = client.containers.list_logs(
            resource_group_name=self.resource_group_name,
            container_group_name=container_group.name,
            container_name=container.name,
            tail=max_lines,
            timestamps=True,
        )

        return logs.content

    def _stream_output(
        self, log_content: str, last_log_time: datetime.datetime
    ) -> datetime.datetime:
        """
        Writes each entry from a string of log lines to stderr.

        Args:
            log_content: A string containing Azure container logs.
            last_log_time: The timestamp of the last output line already streamed.

        Returns:
            The time of the most recent output line written by this call.
        """
        if not log_content:
            # nothing to stream
            return last_log_time

        log_lines = log_content.split("\n")

        last_written_time = last_log_time

        for log_line in log_lines:
            # skip if the line is blank or whitespace
            if not log_line.strip():
                continue

            line_parts = log_line.split(" ")
            # timestamp should always be before first space in line
            line_timestamp = line_parts[0]
            line = " ".join(line_parts[1:])

            try:
                line_time = dateutil.parser.parse(line_timestamp)
                if line_time > last_written_time:
                    self._write_output_line(line)
                    last_written_time = line_time
            except dateutil.parser.ParserError as e:
                self.logger.debug(
                    "Unable to parse timestamp from Azure log line: %s",
                    log_line,
                    exc_info=e,
                )

        return last_written_time

    def _get_environment(self):
        """
        Generates a dictionary of all environment variables to send to the
        ACI container.
        """
        return {**self._base_environment(), **self.env}

    @property
    def _log_prefix(self) -> str:
        """
        Internal property for generating a prefix for logs where `name` may be null
        """
        if self.name is not None:
            return f"AzureContainerInstanceJob {self.name!r}"
        else:
            return "AzureContainerInstanceJob"

    @staticmethod
    def _provisioning_succeeded(container_group: ContainerGroup) -> bool:
        """
        Determines whether ACI container group provisioning was successful.

        Args:
            container_group: a container group returned by the Azure SDK.

        Returns:
            True if provisioning was successful, False otherwise.
        """
        if not container_group:
            return False

        return (
            container_group.provisioning_state
            == ContainerGroupProvisioningState.SUCCEEDED
            and len(container_group.containers) == 1
        )

    @staticmethod
    def _write_output_line(line: str):
        """
        Writes a line of output to stderr.
        """
        print(line, file=sys.stderr)
