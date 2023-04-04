import enum
from collections import deque
from typing import Dict, List, Deque, Optional, Tuple

from brad.config.dbtype import DBType
from brad.data_sync.logical_plan import (
    LogicalDataSyncPlan,
    LogicalDataSyncOperator,
    ExtractDeltas,
    TransformDeltas,
    ApplyDeltas as LogicalApplyDeltas,
)
from brad.data_sync.operators import Operator
from brad.data_sync.operators.apply_deltas import ApplyDeltas
from brad.data_sync.operators.extract_aurora_s3 import ExtractFromAuroraToS3
from brad.data_sync.operators.run_transformation import RunTransformation
from brad.data_sync.physical_plan import PhysicalDataSyncPlan


class PlanConverter:
    """
    Used to convert a logical data sync plan into a physical plan.
    """

    def __init__(self, logical_plan: LogicalDataSyncPlan) -> None:
        self._logical_plan = logical_plan

        self._intermediate_s3_objects: List[str] = []
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
            for phys_dep in pop.physical_dependencies:
                phys_op.add_dependency(phys_dep)
            self._physical_operators.append(phys_op)

        # TODO (!!)
        # Create additional physical operators to "move" the deltas produced by
        # this op into the engines that are needed by the dependee operators.

        # Decrement the dependencies waiting for count on dependees. If the
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


class _DeltaLocation(enum.Enum):
    # The deltas are in tables on Aurora.
    Aurora = "aurora"
    # The deltas are in tables on Redshift.
    Redshift = "redshift"
    # The deltas are in S3 Iceberg tables that are registered in Athena.
    S3AthenaIceberg = "s3_athena_iceberg"
    # The deltas are in S3 text-based files.
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
