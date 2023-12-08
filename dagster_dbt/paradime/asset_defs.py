import json
import shlex
from argparse import Namespace
from typing import (
    Any,
    Callable,
    Dict,
    FrozenSet,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

import dagster._check as check
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetOut,
    AssetsDefinition,
    AutoMaterializePolicy,
    FreshnessPolicy,
    MetadataValue,
    PartitionsDefinition,
    ResourceDefinition,
    multi_asset,
    with_resources,
)
from dagster._annotations import experimental, experimental_param
from dagster._core.definitions.cacheable_assets import (
    AssetsDefinitionCacheableData,
    CacheableAssetsDefinition,
)
from dagster._core.definitions.metadata import MetadataUserInput
from dagster._core.execution.context.init import build_init_resource_context

from dagster_dbt.asset_utils import (
    default_asset_key_fn,
    default_auto_materialize_policy_fn,
    default_description_fn,
    default_freshness_policy_fn,
    default_group_from_dbt_resource_props,
    get_asset_deps,
    get_deps,
)
from dagster_dbt.dagster_dbt_translator import DagsterDbtTranslator

from ..errors import DagsterDbtCloudJobInvariantViolationError
from ..utils import ASSET_RESOURCE_TYPES, result_to_events
from .resources import ParadimeClient, ParadimeClientResource

DAGSTER_DBT_COMPILE_RUN_ID_ENV_VAR = "DBT_DAGSTER_COMPILE_RUN_ID"


class DbtParadimeCacheableAssetsDefinition(CacheableAssetsDefinition):
    def __init__(
        self,
        paradime_resource_def: Union[ParadimeClientResource, ResourceDefinition],
        schedule_name: str,
        node_info_to_asset_key: Callable[[Mapping[str, Any]], AssetKey],
        node_info_to_group_fn: Callable[[Mapping[str, Any]], Optional[str]],
        node_info_to_freshness_policy_fn: Callable[
            [Mapping[str, Any]], Optional[FreshnessPolicy]
        ],
        node_info_to_auto_materialize_policy_fn: Callable[
            [Mapping[str, Any]], Optional[AutoMaterializePolicy]
        ],
        partitions_def: Optional[PartitionsDefinition] = None,
        partition_key_to_vars_fn: Optional[Callable[[str], Mapping[str, Any]]] = None,
    ):
        self._paradime_resource_def: ResourceDefinition = (
            paradime_resource_def.get_resource_definition()
            if isinstance(paradime_resource_def, ParadimeClientResource)
            else paradime_resource_def
        )

        self._paradime: ParadimeClient = (
            paradime_resource_def.process_config_and_initialize().get_paradime_client()
            if isinstance(paradime_resource_def, ParadimeClientResource)
            else paradime_resource_def(build_init_resource_context())
        )
        self._schedule_name = schedule_name
        self._schedule_name_uuid: Optional[str] = None
        self._job_commands: List[str]
        self._job_materialization_command_step: int
        self._node_info_to_asset_key = node_info_to_asset_key
        self._node_info_to_group_fn = node_info_to_group_fn
        self._node_info_to_freshness_policy_fn = node_info_to_freshness_policy_fn
        self._node_info_to_auto_materialize_policy_fn = (
            node_info_to_auto_materialize_policy_fn
        )
        self._partitions_def = partitions_def
        self._partition_key_to_vars_fn = partition_key_to_vars_fn

        super().__init__(unique_id=f"paradime-{schedule_name}")

    def compute_cacheable_data(self) -> Sequence[AssetsDefinitionCacheableData]:
        dbt_nodes, dbt_dependencies = self._get_dbt_nodes_and_dependencies()
        return [self._build_paradime_assets_cacheable_data(dbt_nodes, dbt_dependencies)]

    def build_definitions(
        self, data: Sequence[AssetsDefinitionCacheableData]
    ) -> Sequence[AssetsDefinition]:
        return with_resources(
            [
                self._build_paradime_assets_from_cacheable_data(
                    assets_definition_metadata
                )
                for assets_definition_metadata in data
            ],
            {"paradime": self._paradime_resource_def},
        )

    @staticmethod
    def parse_dbt_command(dbt_command: str) -> Namespace:
        args = shlex.split(dbt_command)[1:]
        try:
            from dbt.cli.flags import Flags, args_to_context

            # nasty hack to get dbt to parse the args
            # dbt >= 1.5.0 requires that profiles-dir is set to an existing directory
            return Namespace(
                **vars(Flags(args_to_context(args + ["--profiles-dir", "."])))
            )
        except ImportError:
            # dbt < 1.5.0 compat
            from dbt.main import parse_args  # type: ignore

            return parse_args(args=args)

    @staticmethod
    def get_job_materialization_command_step(execute_steps: List[str]) -> int:
        materialization_command_filter = [
            DbtParadimeCacheableAssetsDefinition.parse_dbt_command(command).which
            in ["run", "build"]
            for command in execute_steps
        ]

        if sum(materialization_command_filter) != 1:
            raise DagsterDbtCloudJobInvariantViolationError(
                "The paradime schedule name must have a single `dbt run` or `dbt build` in its commands. "
                f"Received commands: {execute_steps}."
            )

        return materialization_command_filter.index(True)

    @staticmethod
    def get_compile_filters(parsed_args: Namespace) -> List[str]:
        dbt_compile_options: List[str] = []

        selected_models = parsed_args.select or []
        if selected_models:
            dbt_compile_options.append(f"--select {' '.join(selected_models)}")

        excluded_models = parsed_args.exclude or []
        if excluded_models:
            dbt_compile_options.append(f"--exclude {' '.join(excluded_models)}")

        selector = getattr(parsed_args, "selector_name", None) or getattr(
            parsed_args, "selector", None
        )
        if selector:
            dbt_compile_options.append(f"--selector {selector}")

        return dbt_compile_options

    def _compile_paradime_job(
        self, paradime_job: Mapping[str, Any]
    ) -> Tuple[int, Optional[int]]:
        # Retrieve the filters options from the paradime schedule name's materialization command.
        #
        # There are three filters: `--select`, `--exclude`, and `--selector`.
        materialization_command = self._job_commands[
            self._job_materialization_command_step
        ]
        parsed_args = DbtParadimeCacheableAssetsDefinition.parse_dbt_command(
            materialization_command
        )
        dbt_compile_options = DbtParadimeCacheableAssetsDefinition.get_compile_filters(
            parsed_args=parsed_args
        )

        # Add the partition variable as a variable to the paradime schedule name command.
        #
        # If existing variables passed through the paradime schedule name's command, an error will be
        # raised. Since these are static variables anyways, they can be moved to the
        # `dbt_project.yml` without loss of functionality.
        #
        # Since we're only doing this to generate the dependency structure, just use an arbitrary
        # partition key (e.g. the last one) to retrieve the partition variable.
        if parsed_args.vars and parsed_args.vars != "{}":
            raise DagsterDbtCloudJobInvariantViolationError(
                f"The paradime schedule name '{paradime_job['name']}' ({paradime_job['id']}) must not have"
                " variables defined from `--vars` in its `dbt run` or `dbt build` command."
                " Instead, declare the variables in the `dbt_project.yml` file. Received commands:"
                f" {self._job_commands}."
            )

        if self._partitions_def and self._partition_key_to_vars_fn:
            last_partition_key = self._partitions_def.get_last_partition_key()
            if last_partition_key is None:
                check.failed("PartitionsDefinition has no partitions")
            partition_var = self._partition_key_to_vars_fn(last_partition_key)

            dbt_compile_options.append(f"--vars '{json.dumps(partition_var)}'")

        # We need to retrieve the dependency structure for the assets in the paradime project.
        # However, we can't just use the dependency structure from the latest run, because
        # this historical structure may not be up-to-date with the current state of the project.
        #
        # By always doing a compile step, we can always get the latest dependency structure.
        # This incurs some latency, but at least it doesn't run through the entire materialization
        # process.
        dbt_compile_command = f"dbt compile {' '.join(dbt_compile_options)}"
        compile_run_dbt_output = self._paradime.run_job_and_poll(
            schedule_name=self._schedule_name,
            cause="Generating software-defined assets for Dagster.",
            steps_override=[dbt_compile_command],
        )

        compile_job_materialization_command_step = (
            len(compile_run_dbt_output.run_details.get("run_steps", [])) or None
        )

        return compile_run_dbt_output.run_id, compile_job_materialization_command_step

    def _get_dbt_nodes_and_dependencies(
        self,
    ) -> Tuple[Mapping[str, Any], Mapping[str, FrozenSet[str]]]:
        """For a given paradime schedule name, fetch the latest run's dependency structure of executed nodes."""
        # Fetch information about the job.
        job = self._paradime.get_schedule_name(schedule_name=self._schedule_name)

        # We constraint the kinds of paradime schedule names that we support running.
        #
        # A simple constraint is that we only support jobs that run multiple steps,
        # but it must contain one of either `dbt run` or `dbt build`.
        #
        # As a reminder, `dbt deps` is automatically run before the job's configured commands.
        # And if the settings are enabled, `dbt docs generate` and `dbt source freshness` can
        # automatically run after the job's configured commands.
        #
        # These commands that execute before and after the job's configured commands do not count
        # towards the single command constraint.
        self._job_commands = job["execute_steps"]
        self._job_materialization_command_step = (
            DbtParadimeCacheableAssetsDefinition.get_job_materialization_command_step(
                execute_steps=self._job_commands
            )
        )

        (
            compile_run_id,
            compile_job_materialization_command_step,
        ) = self._compile_paradime_job(paradime_job=job)

        manifest_json = self._paradime.get_manifest(run_id=compile_run_id)
        run_results_json = self._paradime.get_run_results(run_id=compile_run_id)

        # Filter the manifest to only include the nodes that were executed.
        dbt_nodes: Dict[str, Any] = {
            **manifest_json.get("nodes", {}),
            **manifest_json.get("sources", {}),
            **manifest_json.get("metrics", {}),
        }
        executed_node_ids: Set[str] = set(
            result["unique_id"] for result in run_results_json["results"]
        )

        # If there are no executed nodes, then there are no assets to generate.
        # Inform the user to inspect their paradime schedule name's command.
        if not executed_node_ids:
            raise DagsterDbtCloudJobInvariantViolationError(
                f"The paradime schedule name '{job['name']}' ({job['id']}) does not generate any "
                "software-defined assets. Ensure that your dbt project has nodes to execute, "
                "and that your paradime schedule name's materialization command has the proper filter "
                f"options applied. Received commands: {self._job_commands}."
            )

        # Generate the dependency structure for the executed nodes.
        dbt_dependencies = get_deps(
            dbt_nodes=dbt_nodes,
            selected_unique_ids=executed_node_ids,
            asset_resource_types=ASSET_RESOURCE_TYPES,
        )

        return dbt_nodes, dbt_dependencies

    def _build_paradime_assets_cacheable_data(
        self,
        dbt_nodes: Mapping[str, Any],
        dbt_dependencies: Mapping[str, FrozenSet[str]],
    ) -> AssetsDefinitionCacheableData:
        """Given all of the nodes and dependencies for a paradime schedule name, build the cacheable
        representation that generate the asset definition for the job.
        """

        class CustomDagsterDbtTranslator(DagsterDbtTranslator):
            @classmethod
            def get_asset_key(cls, dbt_resource_props):
                return self._node_info_to_asset_key(dbt_resource_props)

            @classmethod
            def get_description(cls, dbt_resource_props):
                # We shouldn't display the raw sql. Instead, inspect if dbt docs were generated,
                # and attach metadata to link to the docs.
                return default_description_fn(dbt_resource_props, display_raw_sql=False)

            @classmethod
            def get_group_name(cls, dbt_resource_props):
                return self._node_info_to_group_fn(dbt_resource_props)

            @classmethod
            def get_freshness_policy(cls, dbt_resource_props):
                return self._node_info_to_freshness_policy_fn(dbt_resource_props)

            @classmethod
            def get_auto_materialize_policy(cls, dbt_resource_props):
                return self._node_info_to_auto_materialize_policy_fn(dbt_resource_props)

        (
            asset_deps,
            asset_ins,
            asset_outs,
            group_names_by_key,
            freshness_policies_by_key,
            auto_materialize_policies_by_key,
            _,
            fqns_by_output_name,
            metadata_by_output_name,
        ) = get_asset_deps(
            dbt_nodes=dbt_nodes,
            deps=dbt_dependencies,
            # TODO: In the future, allow the IO manager to be specified.
            io_manager_key=None,
            dagster_dbt_translator=CustomDagsterDbtTranslator(),
            manifest=None,
        )

        return AssetsDefinitionCacheableData(
            # TODO: In the future, we should allow additional upstream assets to be specified.
            keys_by_input_name={
                input_name: asset_key
                for asset_key, (input_name, _) in asset_ins.items()
            },
            keys_by_output_name={
                output_name: asset_key
                for asset_key, (output_name, _) in asset_outs.items()
            },
            internal_asset_deps={
                asset_outs[asset_key][0]: asset_deps
                for asset_key, asset_deps in asset_deps.items()
            },
            # We don't rely on a static group name. Instead, we map over the dbt metadata to
            # determine the group name for each asset.
            group_name=None,
            metadata_by_output_name={
                output_name: self._build_paradime_assets_metadata(dbt_metadata)
                for output_name, dbt_metadata in metadata_by_output_name.items()
            },
            # TODO: In the future, we should allow the key prefix to be specified.
            key_prefix=None,
            can_subset=True,
            extra_metadata={
                "job_id": self._schedule_name,
                "job_commands": self._job_commands,
                "job_materialization_command_step": self._job_materialization_command_step,
                "group_names_by_output_name": {
                    asset_outs[asset_key][0]: group_name
                    for asset_key, group_name in group_names_by_key.items()
                },
                "fqns_by_output_name": fqns_by_output_name,
            },
            freshness_policies_by_output_name={
                asset_outs[asset_key][0]: freshness_policy
                for asset_key, freshness_policy in freshness_policies_by_key.items()
            },
            auto_materialize_policies_by_output_name={
                asset_outs[asset_key][0]: auto_materialize_policy
                for asset_key, auto_materialize_policy in auto_materialize_policies_by_key.items()
            },
        )

    def _build_paradime_assets_metadata(
        self, dbt_metadata: Dict[str, Any]
    ) -> MetadataUserInput:
        dbt_metadata["Bolt URL"] = MetadataValue.url("https://app.paradime.io/bolt")
        return dbt_metadata

    def _build_paradime_assets_from_cacheable_data(
        self, assets_definition_cacheable_data: AssetsDefinitionCacheableData
    ) -> AssetsDefinition:
        metadata = cast(
            Mapping[str, Any], assets_definition_cacheable_data.extra_metadata
        )
        schedule_name = cast(str, metadata["job_id"])
        commands = cast(List[str], list(metadata["job_commands"]))
        job_materialization_command_step = cast(
            int, metadata["job_materialization_command_step"]
        )
        group_names_by_output_name = cast(
            Mapping[str, str], metadata["group_names_by_output_name"]
        )
        fqns_by_output_name = cast(
            Mapping[str, List[str]], metadata["fqns_by_output_name"]
        )

        @multi_asset(
            name=f"paradime_{schedule_name}",
            deps=list(
                (assets_definition_cacheable_data.keys_by_input_name or {}).values()
            ),
            outs={
                output_name: AssetOut(
                    key=asset_key,
                    group_name=group_names_by_output_name.get(output_name),
                    freshness_policy=(
                        assets_definition_cacheable_data.freshness_policies_by_output_name
                        or {}
                    ).get(
                        output_name,
                    ),
                    auto_materialize_policy=(
                        assets_definition_cacheable_data.auto_materialize_policies_by_output_name
                        or {}
                    ).get(
                        output_name,
                    ),
                    metadata=(
                        assets_definition_cacheable_data.metadata_by_output_name or {}
                    ).get(output_name),
                    is_required=False,
                )
                for output_name, asset_key in (
                    assets_definition_cacheable_data.keys_by_output_name or {}
                ).items()
            },
            internal_asset_deps={
                output_name: set(asset_deps)
                for output_name, asset_deps in (
                    assets_definition_cacheable_data.internal_asset_deps or {}
                ).items()
            },
            partitions_def=self._partitions_def,
            can_subset=assets_definition_cacheable_data.can_subset,
            required_resource_keys={"paradime"},
            compute_kind="dbt",
        )
        def _assets(context: AssetExecutionContext):
            paradime = cast(ParadimeClient, context.resources.paradime)

            # Add the partition variable as a variable to the paradime schedule name command.
            dbt_options: List[str] = []
            if context.has_partition_key and self._partition_key_to_vars_fn:
                partition_var = self._partition_key_to_vars_fn(context.partition_key)

                dbt_options.append(f"--vars '{json.dumps(partition_var)}'")

            # Prepare the materialization step to be overriden with the selection filter
            materialization_command = commands[job_materialization_command_step]

            # Map the selected outputs to dbt models that should be materialized.
            #
            # HACK: This selection filter works even if an existing `--select` is specified in the
            # paradime schedule name. We take advantage of the fact that the last `--select` will be used.
            #
            # This is not ideal, as the triggered run for the paradime schedule name will still have both
            # `--select` options when displayed in the UI, but parsing the command line argument
            # to remove the initial select using argparse.
            if context.is_subset:
                selected_models = [
                    ".".join(fqns_by_output_name[output_name])
                    for output_name in context.selected_output_names
                ]

                dbt_options.append(f"--select {' '.join(sorted(selected_models))}")

                # If the `--selector` option is used, we need to remove it from the command, since
                # it disables other selection options from being functional.
                #
                # See https://docs.getdbt.com/reference/node-selection/syntax for details.
                split_materialization_command = shlex.split(materialization_command)
                if "--selector" in split_materialization_command:
                    idx = split_materialization_command.index("--selector")

                    materialization_command = " ".join(
                        split_materialization_command[:idx]
                        + split_materialization_command[idx + 2 :]
                    )

            commands[
                job_materialization_command_step
            ] = f"{materialization_command} {' '.join(dbt_options)}".strip()

            # Run the paradime schedule name to rematerialize the assets.
            run_output = paradime.run_job_and_poll(
                schedule_name=schedule_name,
                cause=f"Materializing software-defined assets in Dagster run {context.run_id[:8]}",
                steps_override=commands,
            )

            # Target the materialization step when retrieving run artifacts, rather than assuming
            # that the last step is the correct target.
            #
            # We ignore the commands in front of the materialization command. And again, we ignore
            # the `dbt docs generate` step.
            materialization_command_step = len(
                run_output.run_details.get("run_steps", [])
            )
            materialization_command_step -= (
                len(commands) - job_materialization_command_step - 1
            )
            if run_output.run_details.get("job", {}).get("generate_docs"):
                materialization_command_step -= 1

            # TODO: Assume the run completely fails or completely succeeds.
            # In the future, we can relax this assumption.
            manifest_json = paradime.get_manifest(run_id=run_output.run_id)
            run_results_json = self._paradime.get_run_results(run_id=run_output.run_id)

            for result in run_results_json.get("results", []):
                yield from result_to_events(
                    result=result,
                    docs_url=run_output.docs_url,
                    node_info_to_asset_key=self._node_info_to_asset_key,
                    manifest_json=manifest_json,
                    extra_metadata=None,
                    generate_asset_outputs=True,
                )

        return _assets


@experimental
@experimental_param(param="partitions_def")
@experimental_param(param="partition_key_to_vars_fn")
def load_assets_from_paradime_schedule(
    paradime: Union[ParadimeClientResource, ResourceDefinition],
    schedule_name: str,
    node_info_to_asset_key: Callable[
        [Mapping[str, Any]], AssetKey
    ] = default_asset_key_fn,
    node_info_to_group_fn: Callable[
        [Mapping[str, Any]], Optional[str]
    ] = default_group_from_dbt_resource_props,
    node_info_to_freshness_policy_fn: Callable[
        [Mapping[str, Any]], Optional[FreshnessPolicy]
    ] = default_freshness_policy_fn,
    node_info_to_auto_materialize_policy_fn: Callable[
        [Mapping[str, Any]], Optional[AutoMaterializePolicy]
    ] = default_auto_materialize_policy_fn,
    partitions_def: Optional[PartitionsDefinition] = None,
    partition_key_to_vars_fn: Optional[Callable[[str], Mapping[str, Any]]] = None,
) -> CacheableAssetsDefinition:
    if partition_key_to_vars_fn:
        check.invariant(
            partitions_def is not None,
            "Cannot supply a `partition_key_to_vars_fn` without a `partitions_def`.",
        )

    return DbtParadimeCacheableAssetsDefinition(
        paradime_resource_def=paradime,
        schedule_name=schedule_name,
        node_info_to_asset_key=node_info_to_asset_key,
        node_info_to_group_fn=node_info_to_group_fn,
        node_info_to_freshness_policy_fn=node_info_to_freshness_policy_fn,
        node_info_to_auto_materialize_policy_fn=node_info_to_auto_materialize_policy_fn,
        partitions_def=partitions_def,
        partition_key_to_vars_fn=partition_key_to_vars_fn,
    )
