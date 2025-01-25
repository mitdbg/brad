import enum
from typing import List, Dict, Optional, Set
from pydantic import BaseModel, AwareDatetime

from brad.blueprint import Blueprint
from brad.config.engine import Engine
from brad.vdbe.models import VirtualInfrastructure


class TimestampedMetrics(BaseModel):
    timestamps: List[AwareDatetime]
    values: List[float]


class MetricsData(BaseModel):
    named_metrics: Dict[str, TimestampedMetrics]


class DisplayableTable(BaseModel):
    name: str
    writable: bool = False


class DisplayablePhysicalEngine(BaseModel):
    name: str
    engine: Engine
    provisioning: Optional[str]
    tables: List[DisplayableTable]
    mapped_vdbes: List[str]


class DisplayableBlueprint(BaseModel):
    engines: List[DisplayablePhysicalEngine]

    @classmethod
    def from_blueprint(
        cls, blueprint: Blueprint, virtual_infra: Optional[VirtualInfrastructure] = None
    ) -> "DisplayableBlueprint":
        physical_mapping: Dict[Engine, List[str]] = {}
        writable: Dict[Engine, Set[str]] = {}
        if virtual_infra is not None:
            for vdbe in virtual_infra.engines:
                try:
                    physical_mapping[vdbe.mapped_to].append(vdbe.name)
                except KeyError:
                    physical_mapping[vdbe.mapped_to] = [vdbe.name]

                for table in vdbe.tables:
                    if table.writable:
                        try:
                            writable[vdbe.mapped_to].add(table.name)
                        except KeyError:
                            writable[vdbe.mapped_to] = {table.name}

        engines = []
        aurora = blueprint.aurora_provisioning()
        if aurora.num_nodes() > 0:
            writable_aurora = writable.get(Engine.Aurora, set())
            aurora_tables = [
                DisplayableTable(
                    name=table.name, writable=table.name in writable_aurora
                )
                for table, locations in blueprint.tables_with_locations()
                if Engine.Aurora in locations
            ]
            aurora_tables.sort(key=lambda t: t.name)
            engines.append(
                DisplayablePhysicalEngine(
                    name="Aurora",
                    engine=Engine.Aurora,
                    provisioning=str(aurora),
                    tables=aurora_tables,
                    mapped_vdbes=physical_mapping.get(Engine.Aurora, []),
                )
            )

        redshift = blueprint.redshift_provisioning()
        if redshift.num_nodes() > 0:
            writable_redshift = writable.get(Engine.Redshift, set())
            redshift_tables = [
                DisplayableTable(
                    name=table.name, writable=table.name in writable_redshift
                )
                for table, locations in blueprint.tables_with_locations()
                if Engine.Redshift in locations
            ]
            redshift_tables.sort(key=lambda t: t.name)
            engines.append(
                DisplayablePhysicalEngine(
                    name="Redshift",
                    engine=Engine.Redshift,
                    provisioning=str(redshift),
                    tables=redshift_tables,
                    mapped_vdbes=physical_mapping.get(Engine.Redshift, []),
                )
            )

        writable_athena = writable.get(Engine.Athena, set())
        athena_tables = [
            DisplayableTable(name=table.name, writable=table.name in writable_athena)
            for table, locations in blueprint.tables_with_locations()
            if Engine.Athena in locations
        ]
        athena_tables.sort(key=lambda t: t.name)
        if len(athena_tables) > 0:
            engines.append(
                DisplayablePhysicalEngine(
                    name="Athena",
                    engine=Engine.Athena,
                    provisioning=None,
                    tables=athena_tables,
                    mapped_vdbes=physical_mapping.get(Engine.Athena, []),
                )
            )

        return cls(engines=engines)


class Status(enum.Enum):
    Running = "running"
    Planning = "planning"
    Transitioning = "transitioning"


class SystemState(BaseModel):
    status: Status
    virtual_infra: VirtualInfrastructure
    blueprint: DisplayableBlueprint
    next_blueprint: Optional[DisplayableBlueprint]


class ClientState(BaseModel):
    max_clients: int
    curr_clients: int


class SetClientState(BaseModel):
    runner_port: Optional[int] = None
    curr_clients: int
