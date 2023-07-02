import logging
from collections import deque
from typing import Optional, Tuple

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.data_sync.execution.context import ExecutionContext
from brad.data_sync.execution.plan_converter import PlanConverter
from brad.data_sync.execution.table_sync_bounds import TableSyncBounds
from brad.data_sync.logical_plan import LogicalDataSyncPlan
from brad.data_sync.physical_plan import PhysicalDataSyncPlan
from brad.data_sync.planner import make_logical_data_sync_plan
from brad.server.blueprint_manager import BlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class DataSyncExecutor:
    def __init__(self, config: ConfigFile, blueprint_mgr: BlueprintManager) -> None:
        self._blueprint_mgr = blueprint_mgr
        self._config = config
        self._engines: Optional[EngineConnections] = None

    async def establish_connections(self) -> None:
        logger.debug(
            "Data sync executor is establishing connections to the underlying engines..."
        )
        self._engines = await EngineConnections.connect(
            self._config, self._blueprint_mgr.schema_name, autocommit=False
        )
        # Reads/writes to the data sync metadata are handled by this Aurora connection.
        # We need serializable isolation for correctness.
        conn = self._engines.get_connection(Engine.Aurora)
        cursor = await conn.cursor()
        await cursor.execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        )

    async def shutdown(self) -> None:
        if self._engines is None:
            return
        await self._engines.close()
        logger.debug("Closed connections to the underlying engines.")

    async def run_sync(self, blueprint: Blueprint) -> bool:
        ctx = self._new_execution_context()
        _, phys_plan = await self._get_processed_plans_impl(blueprint, ctx)
        if len(phys_plan.all_operators()) == 0:
            # There is nothing to execute. But we must commit the transaction
            # that looked up the extraction ranges.
            aurora = await ctx.aurora()
            await aurora.commit()
            return False
        await self._run_plan(phys_plan, ctx)
        return True

    def get_static_logical_plan(self, blueprint: Blueprint) -> LogicalDataSyncPlan:
        return make_logical_data_sync_plan(blueprint)

    async def get_processed_plans(
        self, blueprint: Blueprint
    ) -> Tuple[LogicalDataSyncPlan, PhysicalDataSyncPlan]:
        ctx = self._new_execution_context()
        try:
            return await self._get_processed_plans_impl(blueprint, ctx)
        finally:
            aurora = await ctx.aurora()
            await aurora.commit()

    async def _get_processed_plans_impl(
        self, blueprint: Blueprint, ctx: ExecutionContext
    ) -> Tuple[LogicalDataSyncPlan, PhysicalDataSyncPlan]:
        # 1. Get the static logical plan.
        logical = self.get_static_logical_plan(blueprint)

        # 2. Retrieve the sync bounds for the base tables (data sources).
        base_tables = list(map(lambda op: op.table_name(), logical.base_operators()))
        table_bounds = await TableSyncBounds.get_table_sync_bounds_for(base_tables, ctx)
        ctx.set_table_sync_bounds(table_bounds)

        # 3. Process the logical plan: mark base operators as definitely having
        # no results if they correspond to tables that have no changes. Then
        # propagate these markers upwards.
        logger.debug("Table bounds: %s", str(table_bounds))
        for base_op in logical.base_operators():
            base_table = base_op.table_name()
            if (
                base_table not in table_bounds
                or table_bounds[base_table].can_skip_sync()
            ):
                base_op.set_definitely_empty(True)
        logical.propagate_definitely_empty()

        # 4. Prune logical operators that we know will not produce any deltas.
        pruned_logical = logical.prune_empty_ops()

        # 5. Convert the logical plan into a physical plan for later execution.
        converter = PlanConverter(pruned_logical, blueprint)
        return pruned_logical, converter.get_plan()

    async def _run_plan(
        self, plan: PhysicalDataSyncPlan, ctx: ExecutionContext
    ) -> None:
        # 1. Reset all operators (their metadata) to prepare for execution.
        for op in plan.all_operators():
            op.reset_ready_to_run()

        ready_to_run = deque([*plan.base_ops()])

        # Sanity check.
        for op in ready_to_run:
            assert op.ready_to_run()

        # 2. Actually run the operators.
        # Serial execution to begin.
        while len(ready_to_run) > 0:
            op = ready_to_run.popleft()
            await op.execute(ctx)
            # Schedule the next set of operations.
            for dependee in op.dependees():
                dependee.mark_dependency_complete()
                if dependee.ready_to_run():
                    ready_to_run.append(dependee)

    def _new_execution_context(self) -> ExecutionContext:
        assert self._engines is not None
        return ExecutionContext(
            aurora=self._engines.get_connection(Engine.Aurora),
            athena=self._engines.get_connection(Engine.Athena),
            redshift=self._engines.get_connection(Engine.Redshift),
            blueprint=self._blueprint_mgr.get_blueprint(),
            config=self._config,
        )
