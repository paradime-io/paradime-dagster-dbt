from typing import List, Optional

from dagster import Config, In, Nothing, Out, Output, op
from pydantic import Field

from ..utils import generate_materializations
from .resources import DEFAULT_POLL_INTERVAL
from .types import ParadimeOutput


class ParadimeRunOpConfig(Config):
    schedule_name: str = Field(description=("The paradime bolt schedule name"))
    poll_interval: float = Field(
        default=DEFAULT_POLL_INTERVAL,
        description="The time (in seconds) that will be waited between successive polls.",
    )
    poll_timeout: Optional[float] = Field(
        default=None,
        description=(
            "The maximum time that will waited before this operation is timed out. By "
            "default, this will never time out."
        ),
    )
    yield_materializations: bool = Field(
        default=True,
        description=(
            "If True, materializations corresponding to the results of the dbt operation will "
            "be yielded when the op executes."
        ),
    )

    asset_key_prefix: List[str] = Field(
        default=["dbt"],
        description=(
            "If provided and yield_materializations is True, these components will be used to "
            "prefix the generated asset keys."
        ),
    )


@op(
    required_resource_keys={"paradime"},
    ins={"start_after": In(Nothing)},
    out=Out(ParadimeOutput, description="Parsed output from running the paradime job."),
    tags={"kind": "paradime"},
)
def paradime_run_op(context, config: ParadimeRunOpConfig):
    dbt_output = context.resources.paradime.run_job_and_poll(
        config.schedule_name,
        poll_interval=config.poll_interval,
        poll_timeout=config.poll_timeout,
    )
    if config.yield_materializations and "results" in dbt_output.result:
        yield from generate_materializations(
            dbt_output, asset_key_prefix=config.asset_key_prefix
        )
    yield Output(
        dbt_output,
        metadata={
            "created_at": dbt_output.run_details["created_at"],
            "started_at": dbt_output.run_details["started_at"],
            "finished_at": dbt_output.run_details["finished_at"],
            "total_duration": dbt_output.run_details["duration"],
            "run_duration": dbt_output.run_details["run_duration"],
        },
    )
