from typing import List, Dict, Optional
from pydantic import BaseModel, AwareDatetime

from brad.blueprint import Blueprint
from brad.config.engine import Engine


class TimestampedMetrics(BaseModel):
    timestamps: List[AwareDatetime]
    values: List[float]


class MetricsData(BaseModel):
    named_metrics: Dict[str, TimestampedMetrics]


class DisplayableTable(BaseModel):
    name: str
    is_writer: bool = False
    mapped_to: List[str] = []


class DisplayablePhysicalEngine(BaseModel):
    name: str
    provisioning: Optional[str]
    tables: List[DisplayableTable]


class DisplayableBlueprint(BaseModel):
    engines: List[DisplayablePhysicalEngine]

    @classmethod
    def from_blueprint(cls, blueprint: Blueprint) -> "DisplayableBlueprint":
        engines = []
        aurora = blueprint.aurora_provisioning()
        if aurora.num_nodes() > 0:
            aurora_tables = [
                # TODO: Hardcoded Aurora writer. This will change down the road.
                DisplayableTable(name=table.name, is_writer=False)
                for table, locations in blueprint.tables_with_locations()
                if Engine.Aurora in locations
            ]
            aurora_tables.sort(key=lambda t: t.name)
            engines.append(
                DisplayablePhysicalEngine(
                    name="Aurora",
                    provisioning=str(aurora),
                    tables=aurora_tables,
                )
            )

        redshift = blueprint.redshift_provisioning()
        if redshift.num_nodes() > 0:
            redshift_tables = [
                # TODO: Hardcoded Redshift writer. This will change down the road.
                DisplayableTable(name=table.name, is_writer=False)
                for table, locations in blueprint.tables_with_locations()
                if Engine.Redshift in locations
            ]
            redshift_tables.sort(key=lambda t: t.name)
            engines.append(
                DisplayablePhysicalEngine(
                    name="Redshift",
                    provisioning=str(redshift),
                    tables=redshift_tables,
                )
            )

        athena_tables = [
            # TODO: Hardcoded Athena writer. This will change down the road.
            DisplayableTable(name=table.name, is_writer=False)
            for table, locations in blueprint.tables_with_locations()
            if Engine.Athena in locations
        ]
        athena_tables.sort(key=lambda t: t.name)
        if len(athena_tables) > 0:
            engines.append(
                DisplayablePhysicalEngine(
                    name="Athena", provisioning=None, tables=athena_tables
                )
            )

        return cls(engines=engines)


class DisplayableVirtualEngine(BaseModel):
    index: int
    freshness: str
    dialect: str
    peak_latency_s: Optional[float] = None
    tables: List[DisplayableTable] = []


class VirtualInfrastructure(BaseModel):
    engines: List[DisplayableVirtualEngine]


class SystemState(BaseModel):
    virtual_infra: VirtualInfrastructure
    blueprint: DisplayableBlueprint
