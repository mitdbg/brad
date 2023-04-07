import logging
from collections import deque
from typing import Optional

from brad.config.dbtype import DBType
from brad.config.file import ConfigFile
from brad.data_sync.execution.context import ExecutionContext
from brad.data_sync.physical_plan import PhysicalDataSyncPlan
from brad.server.data_blueprint_manager import DataBlueprintManager
from brad.server.engine_connections import EngineConnections

logger = logging.getLogger(__name__)


class DataSyncPlanExecutor:
    def __init__(
        self, config: ConfigFile, data_blueprint_mgr: DataBlueprintManager
    ) -> None:
        self._config = config
        self._data_blueprint_mgr = data_blueprint_mgr
        self._engines: Optional[EngineConnections] = None

    async def establish_connections(self) -> None:
        logger.debug(
            "Data sync executor is establishing connections to the underlying engines..."
        )
        self._engines = await EngineConnections.connect(
            self._config, self._data_blueprint_mgr.schema_name, autocommit=False
        )
        await self._engines.get_connection(DBType.Aurora).execute(
            "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE"
        )

    async def shutdown(self) -> None:
        if self._engines is None:
            return
        await self._engines.close()

    async def run_plan(self, plan: PhysicalDataSyncPlan) -> None:
        # 1. Reset all operators (their metadata) to prepare for execution.
        for op in plan.all_operators():
            op.reset_ready_to_run()

        ctx = self._new_execution_context()
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
            aurora=self._engines.get_connection(DBType.Aurora),
            athena=self._engines.get_connection(DBType.Athena),
            redshift=self._engines.get_connection(DBType.Redshift),
            blueprint=self._data_blueprint_mgr.get_blueprint(),
            config=self._config,
        )
