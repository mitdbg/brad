use arrow::datatypes::SchemaRef;
use datafusion::common::JoinType;
use datafusion::physical_expr::PhysicalExprRef;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_plan::joins::utils::{self, ColumnIndex};
use datafusion::physical_plan::joins::PartitionMode;
use datafusion::physical_plan::Partitioning;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use std::sync::Arc;

#[derive(Debug)]
pub struct InnerHashJoin {
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    on: (PhysicalExprRef, PhysicalExprRef),
    schema: SchemaRef,
    _column_indices: Vec<ColumnIndex>,
    partition_mode: PartitionMode,
}

impl InnerHashJoin {
    pub fn new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        partition_mode: PartitionMode,
    ) -> Self {
        let left_schema = left.schema();
        let right_schema = right.schema();
        let (join_schema, column_indices) =
            utils::build_join_schema(&left_schema, &right_schema, &JoinType::Inner);
        Self {
            left,
            right,
            on,
            schema: SchemaRef::new(join_schema),
            _column_indices: column_indices,
            partition_mode,
        }
    }
}

impl ExecutionPlan for InnerHashJoin {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        match self.partition_mode {
            PartitionMode::Auto => Partitioning::UnknownPartitioning(
                self.right.output_partitioning().partition_count(),
            ),
            PartitionMode::CollectLeft => utils::adjust_right_output_partitioning(
                self.right.output_partitioning(),
                self.left.schema().fields.len(),
            ),
            PartitionMode::Partitioned => self.left.output_partitioning(),
        }
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(InnerHashJoin::new(
            children[0].clone(),
            children[1].clone(),
            self.on.clone(),
            self.partition_mode,
        )))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<datafusion::execution::TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        todo!()
    }
}

impl DisplayAs for InnerHashJoin {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InnerHashJoin")
            }
        }
    }
}
