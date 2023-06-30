from typing import Tuple, List, Dict

from brad.blueprint import Blueprint
from brad.blueprint.provisioning import Provisioning
from brad.blueprint.table import Column, Table
from brad.config.engine import Engine

import brad.proto_gen.blueprint_pb2 as b

# We define the data blueprint serialization/deserialization functions
# separately from the blueprint classes to avoid mixing protobuf code (an
# implementation detail) with the blueprint classes.

# pylint: disable=no-member
# See https://github.com/protocolbuffers/protobuf/issues/10372


def deserialize_blueprint(raw_data: bytes) -> Blueprint:
    proto = b.Blueprint()
    proto.ParseFromString(raw_data)
    return Blueprint(
        schema_name=proto.schema_name,
        table_schemas=list(map(_table_from_proto, proto.tables)),
        table_locations=dict(map(_table_locations_from_proto, proto.tables)),
        aurora_provisioning=_provisioning_from_proto(proto.aurora),
        redshift_provisioning=_provisioning_from_proto(proto.redshift),
        router_provider=None,
    )


def serialize_blueprint(blueprint: Blueprint) -> bytes:
    proto = b.Blueprint(
        schema_name=blueprint.schema_name(),
        tables=map(_tables_with_locations_to_proto, blueprint.tables_with_locations()),
        aurora=_provisioning_to_proto(blueprint.aurora_provisioning()),
        redshift=_provisioning_to_proto(blueprint.redshift_provisioning()),
        policy=None,
    )
    return proto.SerializeToString()


# Implementation details follow.

# Serialization


def _tables_with_locations_to_proto(
    table_with_locations: Tuple[Table, List[Engine]]
) -> b.Table:
    table, locations = table_with_locations
    return b.Table(
        table_name=table.name,
        columns=map(_table_column_to_proto, table.columns),
        locations=map(_location_to_proto, locations),
        dependencies=b.TableDependency(
            source_table_names=table.table_dependencies,
            transform=table.transform_text,
        ),
        indexes=map(_indexed_columns_to_proto, table.secondary_indexed_columns),
    )


def _table_column_to_proto(col: Column) -> b.TableColumn:
    return b.TableColumn(
        name=col.name, data_type=col.data_type, is_primary=col.is_primary
    )


def _location_to_proto(engine: Engine) -> b.Engine:
    if engine == Engine.Aurora:
        return b.Engine.AURORA  # type: ignore
    elif engine == Engine.Redshift:
        return b.Engine.REDSHIFT  # type: ignore
    elif engine == Engine.Athena:
        return b.Engine.ATHENA  # type: ignore
    else:
        return b.Engine.UNKNOWN  # type: ignore


def _provisioning_to_proto(prov: Provisioning) -> b.Provisioning:
    return b.Provisioning(
        instance_type=prov.instance_type(),
        num_nodes=prov.num_nodes(),
    )


def _indexed_columns_to_proto(indexed_columns: Tuple[Column, ...]) -> b.Index:
    return b.Index(column_name=map(lambda col: col.name, indexed_columns))


# Deserialization


def _table_from_proto(table: b.Table) -> Table:
    col_list = list(map(_table_column_from_proto, table.columns))
    col_map = {col.name: col for col in col_list}
    return Table(
        name=table.table_name,
        columns=col_list,
        table_dependencies=list(table.dependencies.source_table_names),
        transform_text=table.dependencies.transform,
        secondary_indexed_columns=list(
            map(lambda idx: _indexed_columns_from_proto(col_map, idx), table.indexes)
        ),
    )


def _table_column_from_proto(col: b.TableColumn) -> Column:
    return Column(name=col.name, data_type=col.data_type, is_primary=col.is_primary)


def _table_locations_from_proto(table: b.Table) -> Tuple[str, List[Engine]]:
    locations = list(map(_location_from_proto, table.locations))
    return (table.table_name, locations)


def _location_from_proto(engine: b.Engine) -> Engine:
    if engine == b.Engine.AURORA:  # type: ignore
        return Engine.Aurora
    elif engine == b.Engine.REDSHIFT:  # type: ignore
        return Engine.Redshift
    elif engine == b.Engine.ATHENA:  # type: ignore
        return Engine.Athena
    else:
        raise RuntimeError("Unsupported data location {}".format(str(engine)))


def _provisioning_from_proto(prov: b.Provisioning) -> Provisioning:
    return Provisioning(
        instance_type=prov.instance_type,
        num_nodes=prov.num_nodes,
    )


def _indexed_columns_from_proto(
    col_map: Dict[str, Column], indexed_columns: b.Index
) -> Tuple[Column, ...]:
    col_list = []
    for col_name in indexed_columns.column_name:
        col_list.append(col_map[col_name])
    return tuple(col_list)
