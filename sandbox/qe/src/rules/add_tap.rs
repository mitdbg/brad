use crate::ops::Tap;
use crate::rules::ShouldInject;
use datafusion::common::tree_node::Transformed;
use datafusion::common::tree_node::TreeNode;
use datafusion::config::ConfigOptions;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub struct AddTap {
    should_inject: ShouldInject,
}

struct AddTapContext {
    did_inject: bool,
}

impl AddTapContext {
    pub fn new() -> Self {
        Self { did_inject: false }
    }
}

impl AddTap {
    pub fn new(should_inject: ShouldInject) -> Self {
        Self { should_inject }
    }

    fn inject_tap(
        &self,
        ctx: &mut AddTapContext,
        plan: Arc<dyn ExecutionPlan>,
    ) -> datafusion::error::Result<Transformed<Arc<dyn ExecutionPlan>>> {
        if ctx.did_inject {
            return Ok(Transformed::No(plan));
        }
        if (self.should_inject)(&plan) {
            let new_node: Arc<dyn ExecutionPlan> = Arc::new(Tap::new(plan.clone()));
            ctx.did_inject = true;
            Ok(Transformed::Yes(new_node))
        } else {
            Ok(Transformed::No(plan))
        }
    }
}

impl PhysicalOptimizerRule for AddTap {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let mut ctx = AddTapContext::new();
        let mut opt_fn = |plan: Arc<dyn ExecutionPlan>| self.inject_tap(&mut ctx, plan);
        plan.transform_down_mut(&mut opt_fn)
    }

    fn name(&self) -> &str {
        "AddTap"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
