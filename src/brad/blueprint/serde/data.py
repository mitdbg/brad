from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.table import Column, Table
from brad.config.dbtype import DBType

import brad.proto_gen.blueprint_pb2 as b

# We define the data blueprint serialization/deserialization functions
# separately from the blueprint classes to avoid mixing protobuf code (an
# implementation detail) with the blueprint classes.

# pylint: disable=no-member
# See https://github.com/protocolbuffers/protobuf/issues/10372


def deserialize_data_blueprint(raw_data: bytes) -> DataBlueprint:
    proto = b.DataBlueprint()
    proto.ParseFromString(raw_data)
    return DataBlueprint(
        schema_name=proto.schema_name,
        tables=list(map(_table_from_proto, proto.tables)),
    )


def serialize_data_blueprint(blueprint: DataBlueprint) -> bytes:
    proto = b.DataBlueprint(
        schema_name=blueprint.schema_name,
        tables=map(_table_to_proto, blueprint.tables),
    )
    return proto.SerializeToString()


# Implementation details follow.

# Serialization


def _table_to_proto(table: Table) -> b.Table:
    return b.Table(
        table_name=table.name,
        columns=map(_table_column_to_proto, table.columns),
        locations=map(_location_to_proto, table.locations),
        dependencies=b.TableDependency(
            source_table_names=table.table_dependencies,
            transform=table.transform_text,
        ),
    )


def _table_column_to_proto(col: Column) -> b.TableColumn:
    return b.TableColumn(
        name=col.name, data_type=col.data_type, is_primary=col.is_primary
    )


def _location_to_proto(engine: DBType) -> b.Engine:
    if engine == DBType.Aurora:
        return b.Engine.AURORA  # type: ignore
    elif engine == DBType.Redshift:
        return b.Engine.REDSHIFT  # type: ignore
    elif engine == DBType.Athena:
        return b.Engine.ATHENA  # type: ignore
    else:
        return b.Engine.UNKNOWN  # type: ignore


# Deserialization


def _table_from_proto(table: b.Table) -> Table:
    return Table(
        name=table.table_name,
        columns=list(map(_table_column_from_proto, table.columns)),
        table_dependencies=list(table.dependencies.source_table_names),
        transform_text=table.dependencies.transform,
        locations=list(map(_location_from_proto, table.locations)),
    )


def _table_column_from_proto(col: b.TableColumn) -> Column:
    return Column(name=col.name, data_type=col.data_type, is_primary=col.is_primary)


def _location_from_proto(engine: b.Engine) -> DBType:
    if engine == b.Engine.AURORA:  # type: ignore
        return DBType.Aurora
    elif engine == b.Engine.REDSHIFT:  # type: ignore
        return DBType.Redshift
    elif engine == b.Engine.ATHENA:  # type: ignore
        return DBType.Athena
    else:
        raise RuntimeError("Unsupported data location {}".format(str(engine)))
