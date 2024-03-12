import asyncio
import uvicorn
import logging
import importlib.resources as pkg_resources
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from typing import Optional, List

import brad.ui.static as brad_app
from brad.blueprint import Blueprint
from brad.blueprint.manager import BlueprintManager
from brad.config.engine import Engine
from brad.config.file import ConfigFile
from brad.daemon.monitor import Monitor
from brad.ui.uvicorn_server import PatchedUvicornServer
from brad.ui.models import (
    MetricsData,
    TimestampedMetrics,
    DisplayableBlueprint,
    SystemState,
    DisplayableVirtualEngine,
    VirtualInfrastructure,
    DisplayableTable,
)
from brad.daemon.front_end_metrics import FrontEndMetric
from brad.daemon.system_event_logger import SystemEventLogger, SystemEventRecord

logger = logging.getLogger(__name__)


class UiManagerImpl:
    def __init__(
        self,
        config: ConfigFile,
        monitor: Monitor,
        blueprint_mgr: BlueprintManager,
        system_event_logger: Optional[SystemEventLogger],
    ) -> None:
        self.config = config
        self.monitor = monitor
        self.blueprint_mgr = blueprint_mgr
        self.system_event_logger = system_event_logger

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
def get_metrics(num_values: int = 3) -> MetricsData:
    assert manager is not None
    metrics = manager.monitor.front_end_metrics().read_k_most_recent(k=num_values)
    qlat = metrics[FrontEndMetric.QueryLatencySecondP90.value]
    qlat_tm = TimestampedMetrics(timestamps=list(qlat.index), values=list(qlat))
    tlat = metrics[FrontEndMetric.TxnLatencySecondP90.value]
    tlat_tm = TimestampedMetrics(timestamps=list(tlat.index), values=list(tlat))
    return MetricsData(
        named_metrics={
            FrontEndMetric.QueryLatencySecondP90.value: qlat_tm,
            FrontEndMetric.TxnLatencySecondP90.value: tlat_tm,
        }
    )


@app.get("/api/1/system_state")
def get_system_state() -> SystemState:
    assert manager is not None
    blueprint = manager.blueprint_mgr.get_blueprint()
    dbp = DisplayableBlueprint.from_blueprint(blueprint)

    # TODO: Hardcoded virtualized infrasturcture and writers.
    txn_tables = ["theatres", "showings", "ticket_orders", "movie_info", "aka_title"]
    txn_only = ["theatres", "showings", "ticket_orders"]
    vdbe1 = DisplayableVirtualEngine(
        name="VDBE 1",
        freshness="Serializable",
        dialect="PostgreSQL SQL",
        peak_latency_s=0.030,
        tables=[
            DisplayableTable(name=name, is_writer=True, mapped_to=["Aurora"])
            for name in [
                "theatres",
                "showings",
                "ticket_orders",
                "movie_info",
                "aka_title",
            ]
        ],
    )
    vdbe1.tables.sort(key=lambda t: t.name)
    vdbe2 = DisplayableVirtualEngine(
        name="VDBE 2",
        freshness="≤ 10 minutes stale",
        dialect="PostgreSQL SQL",
        peak_latency_s=30.0,
        tables=[
            DisplayableTable(
                name=table.name,
                is_writer=False,
                mapped_to=_analytics_table_mapper_temp(table.name, blueprint),
            )
            for table in blueprint.tables()
            if table.name not in txn_only
        ],
    )
    vdbe2.tables.sort(key=lambda t: t.name)
    for engine in dbp.engines:
        if engine.name != "Aurora":
            continue
        for t in engine.tables:
            if t.name in txn_tables:
                t.is_writer = True
    virtual_infra = VirtualInfrastructure(engines=[vdbe1, vdbe2])
    system_state = SystemState(virtual_infra=virtual_infra, blueprint=dbp)
    _add_reverse_mapping_temp(system_state)
    return system_state


def _analytics_table_mapper_temp(table_name: str, blueprint: Blueprint) -> List[str]:
    # TODO: This is a hard-coded heurstic for the mock up only.
    locations = blueprint.get_table_locations(table_name)
    names = []
    if Engine.Redshift in locations:
        names.append("Redshift")
    if Engine.Athena in locations:
        names.append("Athena")
    return names


def _add_reverse_mapping_temp(system_state: SystemState) -> None:
    # TODO: This is a hard-coded heuristic for the mock up only.
    # This mutates the passed-in object.
    veng_tables = {}
    for veng in system_state.virtual_infra.engines:
        table_names = {table.name for table in veng.tables}
        veng_tables[veng.name] = table_names

    for engine in system_state.blueprint.engines:
        for table in engine.tables:
            name = table.name
            for veng_name, tables in veng_tables.items():
                if name in tables:
                    table.mapped_to.append(veng_name)


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
