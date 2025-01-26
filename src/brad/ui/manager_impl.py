import asyncio
import uvicorn
import logging
import importlib.resources as pkg_resources
import numpy as np
import requests
from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from typing import Optional, List
from pydantic import BaseModel

import brad.ui.static as brad_app
from brad.blueprint import Blueprint
from brad.blueprint.table import Table
from brad.blueprint.manager import BlueprintManager
from brad.planner.abstract import BlueprintPlanner
from brad.config.file import ConfigFile
from brad.daemon.monitor import Monitor
from brad.ui.uvicorn_server import PatchedUvicornServer
from brad.ui.models import (
    MetricsData,
    TimestampedMetrics,
    DisplayableBlueprint,
    SystemState,
    Status,
    ClientState,
    SetClientState,
)
from brad.daemon.front_end_metrics import FrontEndMetric
from brad.daemon.system_event_logger import SystemEventLogger, SystemEventRecord
from brad.vdbe.manager import VdbeManager
from brad.vdbe.models import VirtualEngine, CreateVirtualEngineArgs

logger = logging.getLogger(__name__)


class UiManagerImpl:
    def __init__(
        self,
        config: ConfigFile,
        monitor: Monitor,
        blueprint_mgr: BlueprintManager,
        vdbe_mgr: VdbeManager,
        system_event_logger: Optional[SystemEventLogger],
    ) -> None:
        self.config = config
        self.monitor = monitor
        self.blueprint_mgr = blueprint_mgr
        self.vdbe_mgr = vdbe_mgr
        self.system_event_logger = system_event_logger
        self.planner: Optional[BlueprintPlanner] = None

    async def serve_forever(self) -> None:
        global manager  # pylint: disable=global-statement
        try:
            if manager is not None:  # pylint: disable=used-before-assignment
                raise RuntimeError(
                    "Cannot start UiManager while one is already running."
                )

            uvicorn_config = uvicorn.Config(
                "brad.ui.manager_impl:app",
                host=self.config.ui_interface(),
                port=self.config.ui_port(),
                log_level="info",
            )
            server = PatchedUvicornServer(uvicorn_config)
            manager = self
            await server.serve()
        except asyncio.CancelledError:
            logger.info("Shutting down the BRAD UI webserver...")
            server.should_exit = True
        finally:
            manager = None


# Note that because this is global, we can only serve one router instance at a
# time. But this is acceptable given our use cases.
app = FastAPI()
manager: Optional["UiManagerImpl"] = None


@app.get("/api/1/metrics")
def get_metrics(num_values: int = 3, use_generated: bool = False) -> MetricsData:
    assert manager is not None
    metrics = manager.monitor.front_end_metrics().read_k_most_recent(k=num_values)
    qlat = metrics[FrontEndMetric.QueryLatencySecondP90.value]
    qlat_tm = TimestampedMetrics(timestamps=list(qlat.index), values=list(qlat))
    tlat = metrics[FrontEndMetric.TxnLatencySecondP90.value]
    tlat_tm = TimestampedMetrics(timestamps=list(tlat.index), values=list(tlat))

    if use_generated:
        qlat_gen = np.random.normal(loc=15.0, scale=5.0, size=len(qlat))
        tlat_gen = np.random.normal(loc=0.015, scale=0.005, size=len(tlat))
        qlat_tm.values = list(qlat_gen)
        tlat_tm.values = list(tlat_gen)

    return MetricsData(
        named_metrics={
            FrontEndMetric.QueryLatencySecondP90.value: qlat_tm,
            FrontEndMetric.TxnLatencySecondP90.value: tlat_tm,
        }
    )


@app.get("/api/1/system_state")
def get_system_state(filter_tables_for_demo: bool = False) -> SystemState:
    assert manager is not None
    blueprint = manager.blueprint_mgr.get_blueprint()

    # TODO: Hardcoded virtualized infrasturcture and writers.
    txn_tables = ["theatres", "showings", "ticket_orders", "movie_info", "aka_title"]

    if filter_tables_for_demo:
        # To improve how the UI looks in a screenshot, we filter out some tables
        # to reduce the amount of information shown. We keep up to 5 +
        # len(txn_tables) around (upper bound).
        relevant_tables: List[Table] = []
        max_tables = min(5, len(blueprint.tables()))
        for table in blueprint.tables():
            if table.name in txn_tables or len(relevant_tables) < max_tables:
                relevant_tables.append(table)

        new_locations = {}
        for table in relevant_tables:
            new_locations[table.name] = blueprint.get_table_locations(table.name)
        blueprint = Blueprint(
            schema_name=blueprint.schema_name(),
            table_schemas=relevant_tables,
            table_locations=new_locations,
            aurora_provisioning=blueprint.aurora_provisioning(),
            redshift_provisioning=blueprint.redshift_provisioning(),
            full_routing_policy=blueprint.get_routing_policy(),
        )

    virtual_infra = manager.vdbe_mgr.infra()
    dbp = DisplayableBlueprint.from_blueprint(blueprint, virtual_infra)
    status = _determine_current_status(manager)
    if status is Status.Transitioning:
        next_blueprint = manager.blueprint_mgr.get_transition_metadata().next_blueprint
        assert next_blueprint is not None
        next_dbp = DisplayableBlueprint.from_blueprint(next_blueprint)
    else:
        next_dbp = None
    all_tables = [t.name for t in blueprint.tables()]
    all_tables.sort()
    system_state = SystemState(
        status=status,
        virtual_infra=virtual_infra,
        blueprint=dbp,
        next_blueprint=next_dbp,
        all_tables=all_tables,
    )
    return system_state


@app.get("/api/1/clients")
def get_workload_clients(runner_port: Optional[int] = None) -> ClientState:
    # This proxies the request to the runner, which runs as a different process
    # and listens for requests on a different port. We require a proxy to avoid
    # CORS restrictions.
    if runner_port is None:
        # Used for debugging without starting the variable client runner.
        return ClientState(max_clients=12, curr_clients=3)
    else:
        try:
            r = requests.get(f"http://localhost:{runner_port}/clients", timeout=2)
            if r.status_code != 200:
                raise HTTPException(r.status_code, r.reason)
            return ClientState(**r.json())
        except requests.ConnectionError as ex:
            raise HTTPException(400, f"Unable to connect to port {runner_port}") from ex


@app.post("/api/1/clients")
def set_clients(clients: SetClientState) -> ClientState:
    # This proxies the request to the runner, which runs as a different process
    # and listens for requests on a different port. We require a proxy to avoid
    # CORS restrictions.
    if clients.runner_port is None:
        # Used for debugging without starting the variable client runner.
        return ClientState(max_clients=12, curr_clients=clients.curr_clients)
    else:
        try:
            r = requests.post(
                f"http://localhost:{clients.runner_port}/clients",
                json=clients.dict(),
                timeout=2,
            )
            if r.status_code != 200:
                raise HTTPException(r.status_code, r.reason)
            return ClientState(**r.json())
        except requests.ConnectionError as ex:
            raise HTTPException(
                400, f"Unable to connect to port {clients.runner_port}"
            ) from ex


class PredictedChangesArgs(BaseModel):
    t_multiplier: float
    a_multiplier: float


@app.post("/api/1/predicted_changes")
async def get_predicted_changes(args: PredictedChangesArgs) -> DisplayableBlueprint:
    """
    Predict the changed blueprint if the workload intensity changed by the given
    multipliers.
    """
    assert manager is not None
    assert manager.planner is not None
    result = await manager.planner.run_replan_direct(
        intensity_multipliers=(args.t_multiplier, args.a_multiplier)
    )
    if result is None:
        raise HTTPException(500, "Failed to run a replan.")
    blueprint, _ = result
    return DisplayableBlueprint.from_blueprint(blueprint)


@app.post("/api/1/vdbe")
def create_vdbe(engine: CreateVirtualEngineArgs) -> VirtualEngine:
    assert manager is not None
    assert manager.vdbe_mgr is not None

    # Do some simple validation.
    if engine.name == "":
        raise HTTPException(400, "name must be non-empty.")
    if engine.max_staleness_ms < 0:
        raise HTTPException(400, "max_staleness_ms must be non-negative.")
    if engine.p90_latency_slo_ms <= 0:
        raise HTTPException(400, "p90_latency_slo_ms must be positive.")

    return manager.vdbe_mgr.add_engine(engine)


@app.put("/api/1/vdbe")
def update_vdbe(engine: VirtualEngine) -> VirtualEngine:
    assert manager is not None
    assert manager.vdbe_mgr is not None

    # Do some simple validation.
    if engine.name == "":
        raise HTTPException(400, "name must be non-empty.")
    if engine.max_staleness_ms < 0:
        raise HTTPException(400, "max_staleness_ms must be non-negative.")
    if engine.p90_latency_slo_ms <= 0:
        raise HTTPException(400, "p90_latency_slo_ms must be positive.")

    try:
        return manager.vdbe_mgr.update_engine(engine)
    except ValueError as ex:
        raise HTTPException(400, str(ex)) from ex


@app.delete("/api/1/vdbe/{engine_id}")
def delete_vdbe(engine_id: int) -> None:
    assert manager is not None
    assert manager.vdbe_mgr is not None

    try:
        manager.vdbe_mgr.delete_engine(engine_id)
    except ValueError as ex:
        raise HTTPException(400, str(ex)) from ex


def _determine_current_status(manager_impl: UiManagerImpl) -> Status:
    if manager_impl.planner is not None and manager_impl.planner.replan_in_progress():
        return Status.Planning
    if manager_impl.blueprint_mgr.get_transition_metadata().next_blueprint is not None:
        return Status.Transitioning
    return Status.Running


@app.get("/api/1/system_events")
def get_system_events() -> List[SystemEventRecord]:
    assert manager is not None
    return (
        manager.system_event_logger.current_memlog()
        if manager.system_event_logger is not None
        else []
    )


# Serve the static pages.
# Note that this should go last as a "catch all" route.
static_files = pkg_resources.files(brad_app)
with pkg_resources.as_file(static_files) as static_dir:
    app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")
