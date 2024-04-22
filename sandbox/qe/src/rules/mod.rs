use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

mod add_tap;

pub type ShouldInject = fn(&Arc<dyn ExecutionPlan>) -> bool;

pub use add_tap::AddTap;
