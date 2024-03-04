use datafusion::error::DataFusionError;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use std::sync::Arc;

/// "Taps in" to a physical execution plan.
#[derive(Debug)]
pub struct Tap {
    child: Arc<dyn ExecutionPlan>,
}

impl Tap {
    pub fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Tap { child }
    }
}

impl ExecutionPlan for Tap {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.child.output_ordering()
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.child.output_partitioning()
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.child.schema()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        if children.len() != 1 {
            return Err(DataFusionError::NotImplemented(String::from(
                "Cannot tap into multiple child plan nodes.",
            )));
        }
        Ok(Arc::new(Tap {
            child: children.first().unwrap().clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, DataFusionError> {
        self.child.execute(partition, context)
    }
}

impl DisplayAs for Tap {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "Tap")
            }
        }
    }
}
