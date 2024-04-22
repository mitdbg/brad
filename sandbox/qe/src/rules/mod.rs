use std::sync::Arc;
use datafusion::physical_plan::ExecutionPlan;

mod add_tap;
mod add_custom_filter;

pub type ShouldInject = fn(&Arc<dyn ExecutionPlan>) -> bool;

pub use add_tap::AddTap;
pub use add_custom_filter::AddCustomFilter;
