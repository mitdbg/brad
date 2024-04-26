use arrow::record_batch::RecordBatch;
use arrow::compute::filter_record_batch;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::cast::as_boolean_array;
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::context::ExecutionProps;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::logical_expr::{col, lit};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use futures::{Stream, StreamExt};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use datafusion::physical_expr::{create_physical_expr, PhysicalExpr};

/// Example filter to inject into a physical execution plan
#[derive(Debug)]
pub struct Filter {
    child: Arc<dyn ExecutionPlan>,
}

impl Filter {
    pub fn new(child: Arc<dyn ExecutionPlan>) -> Self {
        Filter { child }
    }
}

impl ExecutionPlan for Filter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> arrow::datatypes::SchemaRef {
        self.child.schema()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.child.clone()]
    }

    fn output_partitioning(&self) -> datafusion::physical_plan::Partitioning {
        self.child.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[datafusion::physical_expr::PhysicalSortExpr]> {
        self.child.output_ordering()
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
        Ok(Arc::new(Filter {
            child: children.first().unwrap().clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<datafusion::execution::SendableRecordBatchStream, DataFusionError> {
        // self.child.execute(partition, context)

        // For a logical expression `a = 1`, we can create a physical expression
        let expr = col("o_orderkey").eq(lit(1 as i64));
        // To create a PhysicalExpr we need 1. a schema
        let schema = Schema::new(vec![Field::new("o_orderkey", DataType::Int64, true)]);
        let df_schema = DFSchema::try_from(schema).unwrap();
        // 2. ExecutionProps
        let props = ExecutionProps::new();
        // We can now create a PhysicalExpr:
        let physical_expr = create_physical_expr(&expr, &df_schema, &props).unwrap();
        Ok(Box::pin(FilterStream {
            schema: self.child.schema(),
            predicate: physical_expr.clone(),
            input: self.child.execute(partition, context)?,
        }))
    }
}

impl DisplayAs for Filter {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "TestFilterNode")
            }
        }
    }
}

struct FilterStream {
    /// Output schema, which is the same as the input schema for this operator
    schema: SchemaRef,
    /// The expression to filter on. This expression must evaluate to a boolean value.
    predicate: Arc<dyn PhysicalExpr>,
    /// The input partition to filter.
    input: SendableRecordBatchStream,
}

pub(crate) fn batch_filter(
    batch: &RecordBatch,
    predicate: &Arc<dyn PhysicalExpr>,
) -> Result<RecordBatch, DataFusionError> {
    predicate
        .evaluate(batch)
        .and_then(|v| v.into_array(batch.num_rows()))
        .and_then(|array| {
            Ok(as_boolean_array(&array)?)
                // apply filter array to record batch
                .and_then(|filter_array| Ok(filter_record_batch(batch, filter_array)?))
        })
}

impl Stream for FilterStream {
    type Item = Result<RecordBatch, DataFusionError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll;
        loop {
            match self.input.poll_next_unpin(cx) {
                Poll::Ready(value) => match value {
                    Some(Ok(batch)) => {
                        let filtered_batch = batch_filter(&batch, &self.predicate)?;
                        // skip entirely filtered batches
                        if filtered_batch.num_rows() == 0 {
                            continue;
                        }
                        poll = Poll::Ready(Some(Ok(filtered_batch)));
                        break;
                    }
                    _ => {
                        poll = Poll::Ready(value);
                        break;
                    }
                },
                Poll::Pending => {
                    poll = Poll::Pending;
                    break;
                }
            }
        }
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        // same number of record batches
        self.input.size_hint()
    }
}

impl RecordBatchStream for FilterStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}