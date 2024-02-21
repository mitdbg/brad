use arrow::record_batch::RecordBatch;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionContext;
use datafusion::execution::context::TaskContext;
use datafusion::physical_plan::{self, ExecutionPlan};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

// These constants are implementation details (for use with DataFusion).
const CATALOG_NAME: &str = "brad_qe";
const SCHEMA_NAME: &str = "public";

pub struct DataFusionState {
    ctx: SessionContext,
}

impl DataFusionState {
    pub fn new() -> Self {
        let config = SessionConfig::new()
            .with_default_catalog_and_schema(CATALOG_NAME, SCHEMA_NAME)
            .create_default_catalog_and_schema(true);
        Self {
            ctx: SessionContext::with_config(config),
        }
    }

    pub fn schema_provider(&self) -> Arc<dyn SchemaProvider> {
        self.ctx
            .state
            .read()
            .catalog_list
            .catalog(CATALOG_NAME)
            .unwrap()
            .schema(SCHEMA_NAME)
            .unwrap()
    }

    pub fn session_context(&self) -> &SessionContext {
        &self.ctx
    }

    pub async fn execute_datafusion_plan(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let task_ctx = Arc::new(TaskContext::from(&self.ctx.state()));
        physical_plan::collect(plan, task_ctx).await
    }
}
