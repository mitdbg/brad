import argparse
import yaml
from typing import Any, Dict
from brad.vdbe.models import (
    VirtualInfrastructure,
    VirtualEngine,
    VirtualTable,
    SchemaTable,
    QueryInterface,
)
from brad.config.engine import Engine


# Define your virtual infrastructure here.
def to_serialize(schema: Dict[str, Any]) -> VirtualInfrastructure:
    all_table_names = [tbl["table_name"] for tbl in schema["tables"]]
    t_tables = [
        VirtualTable(name=name, writable=True)
        for name in [
            "theatres",
            "showings",
            "ticket_orders",
            "movie_info",
            "aka_title",
        ]
    ]
    a_tables = [VirtualTable(name=name, writable=False) for name in all_table_names]
    t_engine = VirtualEngine(
        name="VDBE (T)",
        max_staleness_ms=0,
        p90_latency_slo_ms=30,
        interface=QueryInterface.PostgreSQL,
        tables=t_tables,
        mapped_to=Engine.Aurora,
    )
    a_engine = VirtualEngine(
        name="VDBE (A)",
        max_staleness_ms=60 * 60 * 1000,  # 1 hour
        p90_latency_slo_ms=30 * 1000,
        interface=QueryInterface.PostgreSQL,
        tables=a_tables,
        mapped_to=Engine.Redshift,
    )
    return VirtualInfrastructure(
        schema_name=schema["schema_name"],
        engines=[t_engine, a_engine],
        tables=[SchemaTable(name=name) for name in all_table_names],
    )


def main():
    parser = argparse.ArgumentParser(
        description="Tool used to serialize VDBE defintions."
    )
    parser.add_argument("--out-file", type=str, help="Output file path.", required=True)
    parser.add_argument("--compact", action="store_true", help="Compact JSON output.")
    parser.add_argument(
        "--schema-file", type=str, required=True, help="Schema file path."
    )
    args = parser.parse_args()

    with open(args.schema_file, "r", encoding="utf-8") as f:
        schema = yaml.load(f, Loader=yaml.Loader)

    infra = to_serialize(schema)
    indent = None if args.compact else 2
    out_str = infra.model_dump_json(indent=indent)

    with open(args.out_file, "w", encoding="utf-8") as f:
        f.write(out_str)
        f.write("\n")


if __name__ == "__main__":
    main()
