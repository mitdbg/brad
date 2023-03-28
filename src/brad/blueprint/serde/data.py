from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.table import (
    TableLocation,
    TableDependency,
    TableSchema,
    Column,
)
from brad.blueprint.data.location import Location

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
        table_schemas=list(map(_table_schema_from_proto, proto.table_schemas)),
        table_locations=list(map(_table_location_from_proto, proto.table_locations)),
        table_dependencies=list(
            map(_table_dependency_from_proto, proto.table_dependencies)
        ),
    )


def serialize_data_blueprint(blueprint: DataBlueprint) -> bytes:
    proto = b.DataBlueprint(
        schema_name=blueprint.schema_name,
        table_schemas=map(_table_schema_to_proto, blueprint.table_schemas),
        table_locations=map(_table_location_to_proto, blueprint.table_locations),
        table_dependencies=map(
            _table_dependency_to_proto, blueprint.table_dependencies
        ),
    )
    return proto.SerializeToString()


# Implementation details follow.

# Serialization


def _table_schema_to_proto(table: TableSchema) -> b.TableSchema:
    return b.TableSchema(
        table_name=table.name, columns=map(_table_column_to_proto, table.columns)
    )


def _table_column_to_proto(col: Column) -> b.TableColumn:
    return b.TableColumn(
        name=col.name, data_type=col.data_type, is_primary=col.is_primary
    )


def _table_dependency_to_proto(tdep: TableDependency) -> b.TableDependency:
    return b.TableDependency(
        target=_table_location_to_proto(tdep.target),
        sources=map(_table_location_to_proto, tdep.sources),
        transform=tdep.transform,
    )


def _table_location_to_proto(tloc: TableLocation) -> b.TableLocation:
    return b.TableLocation(
        table_name=tloc.table_name, location=_location_to_proto(tloc.location)
    )


def _location_to_proto(loc: Location) -> b.DataLocation:
    if loc == Location.Aurora:
        return b.DataLocation.AURORA  # type: ignore
    elif loc == Location.Redshift:
        return b.DataLocation.REDSHIFT  # type: ignore
    elif loc == Location.S3Iceberg:
        return b.DataLocation.S3_ICEBERG  # type: ignore
    else:
        return b.DataLocation.UNKNOWN  # type: ignore


# Deserialization


def _table_schema_from_proto(table: b.TableSchema) -> TableSchema:
    return TableSchema(
        name=table.table_name,
        columns=list(map(_table_column_from_proto, table.columns)),
    )


def _table_column_from_proto(col: b.TableColumn) -> Column:
    return Column(name=col.name, data_type=col.data_type, is_primary=col.is_primary)


def _table_dependency_from_proto(tdep: b.TableDependency) -> TableDependency:
    return TableDependency(
        target=_table_location_from_proto(tdep.target),
        sources=list(map(_table_location_from_proto, tdep.sources)),
        transform=tdep.transform,
    )


def _table_location_from_proto(tloc: b.TableLocation) -> TableLocation:
    return TableLocation(
        table_name=tloc.table_name, location=_location_from_proto(tloc.location)
    )


def _location_from_proto(loc: b.DataLocation) -> Location:
    if loc == b.DataLocation.AURORA:  # type: ignore
        return Location.Aurora
    elif loc == b.DataLocation.REDSHIFT:  # type: ignore
        return Location.Redshift
    elif loc == b.DataLocation.S3_ICEBERG:  # type: ignore
        return Location.S3Iceberg
    else:
        raise RuntimeError("Unsupported data location {}".format(str(loc)))
