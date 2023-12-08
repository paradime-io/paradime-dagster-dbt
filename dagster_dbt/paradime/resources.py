import datetime
import json
import logging
import time
from enum import Enum
from typing import Any, List, Mapping, Optional, Sequence, Final, Dict

import cachetools
import requests
from dagster import (
    ConfigurableResource,
    Failure,
    IAttachDifferentObjectToOpContext,
    MetadataValue,
    __version__,
    _check as check,
    get_dagster_logger,
    resource,
)
from dagster._core.definitions.resource_definition import dagster_maintained_resource
from pydantic import Field

from .types import ParadimeOutput

# default polling interval (in seconds)
DEFAULT_POLL_INTERVAL = 10

ALREADY_RUNNING_EXCEPTION_STRING: Final = "AlreadyRunningException"


class ParadimeState(str, Enum):
    RUNNING = "Running"
    FAILED = "Failed"
    ERROR = "Error"
    REMOVING = "Removing"
    SUCCESS = "Success"
    FINISHED = "Finished"
    SKIPPED = "Skipped"
    NO_RUNS = "No runs"
    CANCELED = "Canceled"
    PENDING = "Pending"
    STARTING = "Starting"
    DEPLOYING = "Deploying"


class ParadimeClient:
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        paradime_host: str,
        request_max_retries: int = 3,
        request_retry_delay: float = 1,
        log: logging.Logger = get_dagster_logger(),
        log_requests: bool = False,
    ):
        self._api_key = api_key
        self._api_secret = api_secret
        self._paradime_host = paradime_host

        self._request_max_retries = request_max_retries
        self._request_retry_delay = request_retry_delay

        self._log = log
        self._log_requests = log_requests

    def make_request(
        self,
        query: str,
        variables: Dict[str, Any],
    ) -> Any:
        headers = {
            "User-Agent": f"dagster-dbt/{__version__}",
            "Content-Type": "application/json",
            "X-API-KEY": self._api_key,
            "X-API-SECRET": self._api_secret,
        }

        num_retries = 0
        while True:
            try:
                response = requests.post(
                    self._paradime_host,
                    json={"query": query, "variables": variables},
                    timeout=5,  # seconds
                    headers=headers,
                )

                json_data = response.json()
                if "errors" in json_data:
                    raise Failure(f"Error from paradime: {json_data['errors']}")
                if "data" in json_data:
                    return json_data["data"]
                raise Failure(f"Invalid Paradime response: {response.text}")
            except Exception as e:
                if ALREADY_RUNNING_EXCEPTION_STRING not in str(e):
                    self._log.error("Request to paradime API failed: %s", e)
                if num_retries == self._request_max_retries:
                    raise Failure(
                        f"Max retries ({self._request_max_retries}) exceeded: {e}"
                    )
                else:
                    num_retries += 1
                time.sleep(self._request_retry_delay)

    def get_schedule_name(self, schedule_name: str) -> Mapping[str, Any]:
        query = """
            query BoltScheduleNameStatus($scheduleName: String!) {
                boltScheduleName(scheduleName: $scheduleName) {
                    uuid
                    latestRunId
                    commands
                }
            }
        """
        variables = {
            "scheduleName": schedule_name,
        }

        data = self.make_request(query, variables)["boltScheduleName"]

        return {
            "id": data.get("uuid"),
            "runId": data.get("latestRunId"),
            "generate_docs": False,
            "execute_steps": data.get("commands"),
        }

    def run_job(
        self, schedule_name: str, steps_override: List[str], cause: Optional[str] = None
    ) -> Mapping[str, Any]:
        self._log.debug(cause)
        query = """
            mutation TriggerBoltRun($scheduleName: String!, $commands: [String!]) {
                triggerBoltRun(scheduleName: $scheduleName, commands: $commands) {
                    runId
                }
            }
        """
        variables = {
            "scheduleName": schedule_name,
            "commands": steps_override,
        }

        while True:
            try:
                data = self.make_request(query, variables)["triggerBoltRun"]
            except Failure as e:
                if ALREADY_RUNNING_EXCEPTION_STRING in str(e):
                    self._log.info(
                        f"Waiting for a previous bolt run ('{schedule_name}') to finish..."
                    )
                    time.sleep(5)
            else:
                break

        id = data.get("runId")

        resp = {
            "id": id,
            "job": schedule_name,
            "href": get_bolt_url(id),
        }

        self._log.info(
            f"Run initialized with run_id={id}. View this run in "
            f"the Paradime UI: {resp['href']}"
        )
        return resp

    def get_run(self, run_id: int) -> Mapping[str, Any]:
        query = """
            query BoltRunStatus($runId: Int!) {
                boltRunStatus(runId: $runId) {
                    state
                    commands {
                        id
                        command
                    }
                }
            }
        """
        variables = {
            "runId": run_id,
        }

        data = self.make_request(query, variables)["boltRunStatus"]

        state = data.get("state", "Running").capitalize()
        return {
            "steps": [command.get("command") for command in data.get("commands", [])],
            "command_ids": [command.get("id") for command in data.get("commands", [])],
            "id": run_id,
            "href": get_bolt_url(run_id),
            "status_humanized": state,
            "status_message": state,
        }

    def get_run_steps(self, run_id: int) -> Sequence[str]:
        run_details = self.get_run(run_id)
        return run_details["steps"]

    def cancel_run(self, run_id: int) -> None:
        query = """
            mutation CancelBoltRun($scheduleRunId: Int!) {
                cancelBoltRun(scheduleRunId: $scheduleRunId) {
                    ok
                }
            }
        """
        variables = {
            "scheduleRunId": run_id,
        }

        self.make_request(query, variables)

    @cachetools.cached(cache=cachetools.TTLCache(maxsize=1024, ttl=86400))
    def get_run_artifact(self, run_id: int, path: str) -> str:
        self._log.info(f"Fetching run artifact {path} from run id {run_id}...")

        # Get latest command id from run id
        run = self.get_run(run_id)
        command_ids = sorted(run.get("command_ids", []))
        if len(command_ids) < 3:
            self._log.error(f"There are less than 3 commands in the run: {run_id}")

        command_id = command_ids[-1]  # newest command id is going to be the dbt command

        # Get requested resource from command
        resource_id = self._get_resource_id(command_id, path)
        if resource_id is None:
            self._log.warning(
                f"No resource ({path}) found for run {run_id}, command_id {command_id} - "
                f"out of command_ids:{command_ids}"
            )

        # Get downloadable resource url
        query = """
            query BoltResourceUrl($resourceId: Int!) {
                boltResourceUrl(resourceId: $resourceId) {
                    url
                }
            }
        """
        variables = {
            "resourceId": resource_id,
        }
        url = self.make_request(query, variables)["boltResourceUrl"].get("url")

        r = requests.get(url)
        r.raise_for_status()
        return r.text

    def _get_resource_id(
        self, command_id: str, path: str
    ) -> Optional[str]:
        # Get resource id of specific path from command id
        query = """
            query BoltCommand($commandId: Int!) {
                boltCommand(commandId: $commandId) {
                    resources {
                        id
                        path
                    }
                }
            }
        """
        variables = {
            "commandId": command_id,
        }
        data = self.make_request(query, variables)["boltCommand"]
        resource_id = None
        for command_resource in data.get("resources", []):
            if command_resource.get("path") == f"target/{path}":
                resource_id = command_resource.get("id")

        if not resource_id:
            return None
        return resource_id

    def get_manifest(self, run_id: int) -> Mapping[str, Any]:
        return json.loads(self.get_run_artifact(run_id, "manifest.json"))

    def get_run_results(self, run_id: int) -> Mapping[str, Any]:
        return json.loads(self.get_run_artifact(run_id, "run_results.json"))

    def poll_run(
        self,
        run_id: int,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = None,
        href: Optional[str] = None,
    ) -> Mapping[str, Any]:
        # Heavily inspired by cloud.resources.

        status: Optional[str] = None

        if href is None:
            href = self.get_run(run_id).get("href")
        assert isinstance(href, str), "Run must have an href"

        poll_start = datetime.datetime.now()
        try:
            while True:
                run_details = self.get_run(run_id)
                status = run_details["status_humanized"]
                self._log.info(
                    f"Polled run {run_id}. Status: [{status}]. {get_bolt_url(run_id)}"
                )

                # completed successfully
                if status == ParadimeState.SUCCESS:
                    return self.get_run(run_id)
                elif status in [
                    ParadimeState.ERROR,
                    ParadimeState.CANCELED,
                    ParadimeState.FAILED,
                ]:
                    break
                elif status not in [
                    ParadimeState.STARTING,
                    ParadimeState.RUNNING,
                ]:
                    check.failed(
                        f"Received unexpected status '{status}'. This should never happen"
                    )

                if (
                    poll_timeout
                    and datetime.datetime.now()
                    > poll_start + datetime.timedelta(seconds=poll_timeout)
                ):
                    self.cancel_run(run_id)
                    raise Failure(
                        f"Run {run_id} timed out after "
                        f"{datetime.datetime.now() - poll_start}. Attempted to cancel.",
                        metadata={"run_page_url": MetadataValue.url(href)},
                    )

                # Sleep for the configured time interval before polling again.
                time.sleep(poll_interval)
        finally:
            if status not in (
                ParadimeState.SUCCESS,
                ParadimeState.ERROR,
                ParadimeState.FAILED,
                ParadimeState.CANCELED,
            ):
                self._log.info(f"Invalid state ({status}) cancelling...")
                self.cancel_run(run_id)

        run_details = self.get_run(run_id)
        raise Failure(
            f"Run {run_id} failed. Status Message: {run_details['status_message']}",
            metadata={
                "run_details": MetadataValue.json(run_details),
                "run_page_url": MetadataValue.url(href),
            },
        )

    def run_job_and_poll(
        self,
        schedule_name: str,
        poll_interval: float = DEFAULT_POLL_INTERVAL,
        poll_timeout: Optional[float] = None,
        **kwargs,
    ) -> ParadimeOutput:
        run_details = self.run_job(schedule_name, **kwargs)
        run_id = run_details["id"]
        href = run_details["href"]
        final_run_details = self.poll_run(
            run_id, poll_interval=poll_interval, poll_timeout=poll_timeout, href=href
        )
        try:
            run_results = self.get_run_results(run_id)
        # if you fail to get run_results for this job, just leave it empty
        except Failure:
            self._log.info(
                "run_results.json not available for this run. Defaulting to empty value."
            )
            run_results = {}
        output = ParadimeOutput(run_details=final_run_details, result=run_results)
        return output


class ParadimeResource(ParadimeClient):
    pass


class ParadimeClientResource(ConfigurableResource, IAttachDifferentObjectToOpContext):
    """This resource helps interact with paradime connectors."""

    api_key: str = Field(
        description=(
            "Paradime API Token. https://docs.paradime.io/app-help/app-settings/generate-api-keys"
        ),
    )
    api_secret: str = Field(
        description=(
            "Paradime API Secret. https://docs.paradime.io/app-help/app-settings/generate-api-keys"
        ),
    )
    paradime_api_host: str = Field(
        description=(
            "The API endpoint where Paradime is being hosted (e.g. https://api.paradime.io/api/v1/.../graphql). "
            "https://docs.paradime.io/app-help/app-settings/generate-api-keys"
        ),
    )
    request_max_retries: int = Field(
        default=3,
        description=(
            "The maximum number of times requests to the pardime API should be retried "
            "before failing."
        ),
    )
    request_retry_delay: float = Field(
        default=0.25,
        description="Time (in seconds) to wait between each request retry.",
    )

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    def get_paradime_client(self) -> ParadimeClient:
        context = self.get_resource_context()
        assert context.log

        return ParadimeClient(
            api_key=self.api_key,
            api_secret=self.api_secret,
            request_max_retries=self.request_max_retries,
            request_retry_delay=self.request_retry_delay,
            log=context.log,
            paradime_host=self.paradime_api_host,
        )

    def get_object_to_set_on_execution_context(self) -> Any:
        return self.get_paradime_client()


@dagster_maintained_resource
@resource(
    config_schema=ParadimeClientResource.to_config_schema(),
    description="This resource helps interact with pardime connectors",
)
def paradime_resource(context) -> ParadimeResource:
    return ParadimeResource(
        api_secret=context.resource_config["api_secret"],
        api_key=context.resource_config["api_key"],
        request_max_retries=context.resource_config["request_max_retries"],
        request_retry_delay=context.resource_config["request_retry_delay"],
        log=context.log,
        paradime_host=context.resource_config["paradime_host"],
    )


def get_bolt_url(run_id: int) -> str:
    return f"https://app.paradime.io/bolt/run_id/{run_id}"
