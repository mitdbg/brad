use crate::ops::Filter;
use crate::rules::ShouldInject;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub struct AddCustomFilter {
    should_inject: ShouldInject,
}

struct AddCustomFilterContext {
    did_inject: bool,
}

impl AddCustomFilterContext {
    pub fn new() -> Self {
        Self { did_inject: false }
    }
}

impl AddCustomFilter {
    pub fn new(should_inject: ShouldInject) -> Self {
        Self { should_inject }
    }

    fn inject_custom_filter(
        &self,
        ctx: &mut AddCustomFilterContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if ctx.did_inject {
            return Ok(Transformed::No(plan));
        }
        if (self.should_inject)(&plan) {
            let new_node: Arc<dyn ExecutionPlan> = Arc::new(Filter::new(plan.clone()));
            ctx.did_inject = true;
            Ok(Transformed::Yes(new_node))
        } else {
            Ok(Transformed::No(plan))
        }
    }
}

impl PhysicalOptimizerRule for AddCustomFilter {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = AddCustomFilterContext::new();
        let mut opt_fn = |plan: Arc<dyn ExecutionPlan>| self.inject_custom_filter(&mut ctx, plan);
        plan.transform_down_mut(&mut opt_fn)
    }

    fn name(&self) -> &str {
        "AddCustomFilter"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
