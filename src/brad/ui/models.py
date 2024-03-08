from typing import List, Dict, Optional
from pydantic import BaseModel, AwareDatetime

from brad.blueprint import Blueprint
from brad.config.engine import Engine


class TimestampedMetrics(BaseModel):
    timestamps: List[AwareDatetime]
    values: List[float]


class MetricsData(BaseModel):
    named_metrics: Dict[str, TimestampedMetrics]


class DisplayablePhysicalEngine(BaseModel):
    name: str
    provisioning: Optional[str]
    tables: List[str]


class DisplayableBlueprint(BaseModel):
    engines: List[DisplayablePhysicalEngine]

    @classmethod
    def from_blueprint(cls, blueprint: Blueprint) -> "DisplayableBlueprint":
        engines = []
        aurora = blueprint.aurora_provisioning()
        if aurora.num_nodes() > 0:
            aurora_tables = [
                table.name
                for table, locations in blueprint.tables_with_locations()
                if Engine.Aurora in locations
            ]
            aurora_tables.sort()
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
                table.name
                for table, locations in blueprint.tables_with_locations()
                if Engine.Redshift in locations
            ]
            redshift_tables.sort()
            engines.append(
                DisplayablePhysicalEngine(
                    name="Redshift",
                    provisioning=str(redshift),
                    tables=redshift_tables,
                )
            )

        athena_tables = [
            table.name
            for table, locations in blueprint.tables_with_locations()
            if Engine.Athena in locations
        ]
        athena_tables.sort()
        if len(athena_tables) > 0:
            engines.append(
                DisplayablePhysicalEngine(
                    name="Athena", provisioning=None, tables=athena_tables
                )
            )

        return cls(engines=engines)
