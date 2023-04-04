import enum
from collections import deque
from typing import Dict, List, Deque, Optional, Tuple

from brad.blueprint.data import DataBlueprint
from brad.blueprint.data.table import Location
from brad.config.dbtype import DBType
from brad.config.strings import insert_delta_table_name, delete_delta_table_name
from brad.data_sync.logical_plan import (
    LogicalDataSyncPlan,
    LogicalDataSyncOperator,
    ExtractDeltas,
    TransformDeltas,
    ApplyDeltas as LogicalApplyDeltas,
)
from brad.data_sync.operators import Operator
from brad.data_sync.operators.adjust_deltas import AdjustDeltas
from brad.data_sync.operators.apply_deltas import ApplyDeltas
from brad.data_sync.operators.create_temp_table import CreateTempTable
from brad.data_sync.operators.extract_aurora_s3 import ExtractFromAuroraToS3
from brad.data_sync.operators.load_from_s3 import LoadFromS3
from brad.data_sync.operators.register_athena_s3_table import RegisterAthenaS3Table
from brad.data_sync.operators.run_transformation import RunTransformation
from brad.data_sync.operators.unload_to_s3 import UnloadToS3
from brad.data_sync.physical_plan import PhysicalDataSyncPlan


class PlanConverter:
    """
    Used to convert a logical data sync plan into a physical plan.
    """

    def __init__(
        self, logical_plan: LogicalDataSyncPlan, blueprint: DataBlueprint
    ) -> None:
        self._logical_plan = logical_plan
        self._blueprint = blueprint

        self._intermediate_s3_objects: List[str] = []
        self._intermediate_tables: List[Tuple[str, DBType]] = []
        self._processing_ops: Dict[LogicalDataSyncOperator, _ProcessingOp] = {}
        self._ready_to_process: Deque[_ProcessingOp] = deque()

        self._extract_op: Optional[ExtractFromAuroraToS3] = None
        self._physical_operators: List[Operator] = []

    def get_plan(self) -> PhysicalDataSyncPlan:
        tables_to_extract = {}
        for op in self._logical_plan.base_operators():
            # Sanity check.
            assert isinstance(op, ExtractDeltas)

            # Set up the extraction location(s).
            extract_paths = self._s3_extract_paths_for(op.table_name().value)
            tables_to_extract[op.table_name().value] = extract_paths

            # Create a processing op so that we can generate the correct
            # physical operators for dependees.
            pop = _ProcessingOp(op)
            pop.output_location = _DeltaLocation.S3Text
            (
                pop.output_insert_delta_s3_path,
                pop.output_delete_delta_s3_path,
            ) = extract_paths
            self._processing_ops[op] = pop
            self._ready_to_process.append(pop)

            # Keep track of these intermediate objects so that they can be
            # deleted afterwards.
            self._intermediate_s3_objects.append(extract_paths[0])
            self._intermediate_s3_objects.append(extract_paths[1])

        # The base operators should be `ExtractDeltas` operators. We create one
        # Aurora extraction physical operator to ensure that we extract a
        # transactionally-consistent snapshot. This is the base op.
        self._extract_op = ExtractFromAuroraToS3(tables_to_extract)
        self._physical_operators.append(self._extract_op)

        # Process operations until they have all be processed.
        while len(self._ready_to_process) > 0:
            pop = self._ready_to_process.popleft()
            self._process_logical_op(pop)

        return PhysicalDataSyncPlan(self._extract_op, self._physical_operators)

    def reset(self) -> None:
        self._intermediate_s3_objects.clear()
        self._intermediate_tables.clear()
        self._processing_ops.clear()
        self._ready_to_process.clear()
        self._physical_operators = []

    def _process_logical_op(self, pop: "_ProcessingOp") -> None:
        # Get a list of this logical operator's dependees.
        dependees: List[_ProcessingOp] = []
        for dependee in pop.logical_op.dependees():
            if dependee not in self._processing_ops:
                dependee_pop = _ProcessingOp(dependee)
                # Add the deltas' location.
                if isinstance(pop.logical_op, TransformDeltas):
                    dependee_pop.output_location = _DeltaLocation.from_engine(
                        pop.logical_op.engine()
                    )
                elif isinstance(pop.logical_op, LogicalApplyDeltas):
                    # This operator does not "produce" deltas.
                    pass
                elif isinstance(pop.logical_op, ExtractDeltas):
                    # All `ExtractDeltas` should be base operators. They should
                    # not be a dependee.
                    raise AssertionError
                else:
                    raise AssertionError
                self._processing_ops[dependee] = dependee_pop
                dependees.append(dependee_pop)
            else:
                dependees.append(self._processing_ops[dependee])

        # Create a physical operator for this logical op (transform or apply
        # delta). Extractions are a special case and are already handled.
        if isinstance(pop.logical_op, TransformDeltas):
            phys_op: Optional[Operator] = RunTransformation(
                pop.logical_op.transform_text(), pop.logical_op.engine()
            )
        elif isinstance(pop.logical_op, LogicalApplyDeltas):
            phys_op = ApplyDeltas(
                pop.logical_op.table_name().value,
                pop.logical_op.location().default_engine(),
            )
        elif isinstance(pop.logical_op, ExtractDeltas):
            phys_op = None
        else:
            raise AssertionError

        # Register the operator's dependencies.
        if phys_op is not None:
            if isinstance(pop.logical_op, TransformDeltas):
                # Need to create the destination delta tables.
                transform_dest = pop.logical_op.table_name().value
                transform_dest_table = self._blueprint.get_table(transform_dest)
                transform_engine = pop.logical_op.engine()
                phys_op.add_dependency(
                    CreateTempTable(
                        insert_delta_table_name(transform_dest),
                        transform_dest_table.columns,
                        transform_engine,
                    )
                )
                phys_op.add_dependency(
                    CreateTempTable(
                        delete_delta_table_name(transform_dest),
                        transform_dest_table.columns,
                        transform_engine,
                    )
                )
                self._intermediate_tables.extend(
                    [
                        (
                            insert_delta_table_name(transform_dest),
                            transform_engine,
                        ),
                        (
                            delete_delta_table_name(transform_dest),
                            transform_engine,
                        ),
                    ]
                )

            for phys_dep in pop.physical_dependencies:
                phys_op.add_dependency(phys_dep)
            self._physical_operators.append(phys_op)
        else:
            assert self._extract_op is not None
            phys_op = self._extract_op

        if len(dependees) == 0:
            # No dependees - no further processing required.
            return

        # Create additional physical operators to "move" the deltas produced by
        # this op into the engines that are needed by the dependee operators.
        assert pop.output_location is not None
        to_dest: Dict[DBType, Optional[Operator]] = {}
        for dependee_pop in dependees:
            if isinstance(dependee_pop.logical_op, TransformDeltas):
                dest = dependee_pop.logical_op.engine()
            elif isinstance(dependee_pop.logical_op, LogicalApplyDeltas):
                dest = dependee_pop.logical_op.location().default_engine()
            else:
                # Should not have any other kinds of logical ops.
                raise AssertionError

            if dest in to_dest:
                # The ops have already been created.
                self._attach_movement_ops(
                    source_op=pop,
                    dependee=dependee_pop,
                    source_phys_op=phys_op,
                    to_dest_phys_op=to_dest[dest],
                )
                continue

            # Generate the movement ops and then register them.
            ops = self._generate_movement_ops(
                source_op=pop,
                source=pop.output_location,
                dest=dest,
            )
            if len(ops) == 0:
                to_dest[dest] = None
                self._attach_movement_ops(
                    source_op=pop,
                    dependee=dependee_pop,
                    source_phys_op=phys_op,
                    to_dest_phys_op=None,
                )
            else:
                to_dest[dest] = ops[-1]
                # The operator chain depends on this physical operator finishing.
                ops[0].add_dependency(phys_op)
                # Keep track of all the operators we have created.
                self._physical_operators.extend(ops)
                self._attach_movement_ops(
                    source_op=pop,
                    dependee=dependee_pop,
                    source_phys_op=phys_op,
                    to_dest_phys_op=ops[-1],
                )

        # Decrement the "dependencies waiting for count" on dependees. If the
        # count is now zero, schedule it for processing.
        for dependee_pop in dependees:
            # Sanity check.
            assert dependee_pop.dependencies_left_to_process > 0
            dependee_pop.dependencies_left_to_process -= 1
            if dependee_pop.dependencies_left_to_process == 0:
                self._ready_to_process.append(dependee_pop)

    def _s3_extract_paths_for(self, table_name: str) -> Tuple[str, str]:
        prefix = "aurora_extract/{}/".format(table_name)
        return (prefix + "writes/", prefix + "deletes/")

    def _attach_movement_ops(
        self,
        source_op: "_ProcessingOp",
        dependee: "_ProcessingOp",
        source_phys_op: Operator,
        to_dest_phys_op: Optional[Operator],
    ) -> None:
        # The purpose of this method is to attach the physical delta movement
        # operators to the dependency graph, and to add an `AdjustDeltas`
        # operator when needed.
        need_adjust_deltas = isinstance(source_op, ExtractDeltas) and isinstance(
            dependee, TransformDeltas
        )

        if need_adjust_deltas:
            assert isinstance(dependee.logical_op, TransformDeltas)
            dest_table = dependee.logical_op.table_name().value
            dest_table_engine = dependee.logical_op.engine()
            adjust_delta = AdjustDeltas(dest_table, dest_table_engine)
            self._physical_operators.append(adjust_delta)

            if to_dest_phys_op is None:
                adjust_delta.add_dependency(source_phys_op)
                dependee.physical_dependencies.append(adjust_delta)
            else:
                adjust_delta.add_dependency(to_dest_phys_op)
                dependee.physical_dependencies.append(adjust_delta)

        else:
            if to_dest_phys_op is None:
                dependee.physical_dependencies.append(source_phys_op)
            else:
                dependee.physical_dependencies.append(to_dest_phys_op)

    def _generate_movement_ops(
        self,
        source_op: "_ProcessingOp",
        source: "_DeltaLocation",
        dest: DBType,
    ) -> List[Operator]:
        out_ops: List[Operator] = []
        table_name = source_op.logical_op.table_name().value
        table = self._blueprint.get_table(table_name)

        id_table_name = insert_delta_table_name(table_name)
        dd_table_name = delete_delta_table_name(table_name)

        if source == _DeltaLocation.S3Text and dest == DBType.Redshift:
            # 1. Create tables on Redshift
            # 2. Import from S3
            assert source_op.output_insert_delta_s3_path is not None
            assert source_op.output_delete_delta_s3_path is not None
            c1 = CreateTempTable(
                id_table_name,
                table.columns,
                engine=DBType.Redshift,
            )
            c2 = CreateTempTable(
                dd_table_name,
                table.primary_key,
                engine=DBType.Redshift,
            )
            l1 = LoadFromS3(
                id_table_name,
                source_op.output_insert_delta_s3_path,
                engine=DBType.Redshift,
            )
            l2 = LoadFromS3(
                dd_table_name,
                source_op.output_delete_delta_s3_path,
                engine=DBType.Redshift,
            )
            c2.add_dependency(c1)
            l1.add_dependency(c2)
            l2.add_dependency(l1)
            out_ops.extend([c1, c2, l1, l2])
            self._intermediate_tables.extend(
                [(id_table_name, DBType.Redshift), (dd_table_name, DBType.Redshift)]
            )

        elif source == _DeltaLocation.S3Text and dest == DBType.Athena:
            # 1. Register the S3 text data as Athena tables
            assert source_op.output_insert_delta_s3_path is not None
            assert source_op.output_delete_delta_s3_path is not None
            r1 = RegisterAthenaS3Table(
                id_table_name, table.columns, source_op.output_insert_delta_s3_path
            )
            r2 = RegisterAthenaS3Table(
                dd_table_name, table.primary_key, source_op.output_delete_delta_s3_path
            )
            r2.add_dependency(r1)
            out_ops.extend([r1, r2])
            self._intermediate_tables.extend(
                [(id_table_name, DBType.Athena), (dd_table_name, DBType.Athena)]
            )

        elif source == _DeltaLocation.Redshift and dest == DBType.Athena:
            # 1. Unload to S3
            # 2. Register Athena tables
            insert_s3_path = "redshift_unload/{}/inserts/table.tbl".format(table_name)
            delete_s3_path = "redshift_unload/{}/deletes/table.tbl".format(table_name)
            u1 = UnloadToS3(id_table_name, insert_s3_path, engine=DBType.Redshift)
            u2 = UnloadToS3(dd_table_name, delete_s3_path, engine=DBType.Redshift)
            r1 = RegisterAthenaS3Table(id_table_name, table.columns, insert_s3_path)
            r2 = RegisterAthenaS3Table(dd_table_name, table.primary_key, delete_s3_path)
            u2.add_dependency(u1)
            r1.add_dependency(u2)
            r2.add_dependency(r1)
            out_ops.extend([u1, u2, r1, r2])
            self._intermediate_s3_objects.extend([insert_s3_path, delete_s3_path])

        elif source == _DeltaLocation.Redshift and dest == DBType.Aurora:
            # 1. Unload to S3
            # 2. Create Aurora tables
            # 3. Import from S3
            insert_s3_path = "redshift_unload/{}/inserts/table.tbl".format(table_name)
            delete_s3_path = "redshift_unload/{}/deletes/table.tbl".format(table_name)
            u1 = UnloadToS3(id_table_name, insert_s3_path, engine=DBType.Redshift)
            u2 = UnloadToS3(dd_table_name, delete_s3_path, engine=DBType.Redshift)
            c1 = CreateTempTable(id_table_name, table.columns, engine=DBType.Aurora)
            c2 = CreateTempTable(dd_table_name, table.primary_key, engine=DBType.Aurora)
            l1 = LoadFromS3(id_table_name, insert_s3_path, engine=DBType.Aurora)
            l2 = LoadFromS3(dd_table_name, delete_s3_path, engine=DBType.Aurora)
            u2.add_dependency(u1)
            c1.add_dependency(u2)
            c2.add_dependency(c1)
            l1.add_dependency(c2)
            l2.add_dependency(l1)
            out_ops.extend([u1, u2, l1, l2, c1, c2])
            self._intermediate_s3_objects.extend([insert_s3_path, delete_s3_path])

        elif (
            (source == _DeltaLocation.Aurora and dest == DBType.Aurora)
            or (source == _DeltaLocation.Redshift and dest == DBType.Redshift)
            or (source == _DeltaLocation.S3AthenaIceberg and dest == DBType.Athena)
        ):
            # No movement ops needed.
            pass

        else:
            # Some unimplemented transitions:
            # - S3Text -> Aurora (only occurs if we run a transform on Aurora
            #   right after extraction)
            # - Athena -> Redshift
            # - Athena -> Aurora
            raise RuntimeError("Unsupported source/dest: {} -> {}".format(source, dest))

        return out_ops


class _DeltaLocation(enum.Enum):
    # The deltas are in tables on Aurora.
    Aurora = "aurora"
    # The deltas are in tables on Redshift.
    Redshift = "redshift"
    # The deltas are in S3 Iceberg tables that are registered in Athena.
    S3AthenaIceberg = "s3_athena_iceberg"
    # The deltas are in S3 text-based tables that are registered in Athena.
    S3AthenaText = "s3_athena_text"
    # The deltas are in S3 text-based files (unregistered on Athena).
    S3Text = "s3_text"

    @classmethod
    def from_engine(cls, engine: DBType) -> "_DeltaLocation":
        if engine == DBType.Aurora:
            return _DeltaLocation.Aurora
        elif engine == DBType.Athena:
            return _DeltaLocation.S3AthenaIceberg
        elif engine == DBType.Redshift:
            return _DeltaLocation.Redshift
        else:
            raise AssertionError

    @classmethod
    def from_location(cls, location: Location) -> "_DeltaLocation":
        if location == Location.Aurora:
            return _DeltaLocation.Aurora
        elif location == Location.Redshift:
            return _DeltaLocation.Redshift
        elif location == Location.S3Iceberg:
            return _DeltaLocation.S3AthenaIceberg
        else:
            raise AssertionError


class _ProcessingOp:
    """
    A wrapper around a `LogicalDataSyncOperator` that is being lowered into
    physical operators. This class stores metadata used during the lowering
    process.
    """

    def __init__(self, logical_op: LogicalDataSyncOperator) -> None:
        self.logical_op = logical_op
        self.physical_dependencies: List[Operator] = []
        self.dependencies_left_to_process = len(self.logical_op.dependencies())

        # Where the output of `logical_op`'s deltas will be.
        # This is always `_DeltaLocation.S3Text` for `ExtractDeltas`
        # This depends on the transformation engine for `TransformDeltas`
        # This is `None`` for `LogicalApplyDeltas`
        self.output_location: Optional[_DeltaLocation] = None

        # These are relative to the configured S3 extract path.
        self.output_insert_delta_s3_path: Optional[str] = None
        self.output_delete_delta_s3_path: Optional[str] = None
