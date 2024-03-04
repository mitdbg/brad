use crate::ops::Tap;
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

pub type ShouldInject = fn(&Arc<dyn ExecutionPlan>) -> bool;

/// Adds a `Tap` operator into the given `ExecutionPlan` tree based on when
/// `should_inject` returns true.
pub fn inject_tap(
    plan: &Arc<dyn ExecutionPlan>,
    should_inject: ShouldInject,
) -> Result<Option<Arc<dyn ExecutionPlan>>, DataFusionError> {
    // TODO: We should figure out the idiomatic way of rewriting physical
    // DataFusion plans. This function implements a manual DFS traversal.

    // Plan node, its parent, and visit count.
    let mut stack: Vec<(Arc<dyn ExecutionPlan>, Option<Arc<dyn ExecutionPlan>>, u32)> =
        vec![(plan.clone(), None, 0)];

    let mut is_injecting = false;
    let mut injected_plan: Option<Arc<dyn ExecutionPlan>> = None;
    let mut orig_child: Option<Arc<dyn ExecutionPlan>> = None;
    let mut next_parent: Option<Arc<dyn ExecutionPlan>> = None;

    while stack.len() > 0 {
        let (node, parent, visit_count) = stack.pop().unwrap();
        if visit_count == 0 {
            // Pre-visit.
            stack.push((node.clone(), parent, 1));
            for child in &node.children() {
                stack.push((child.clone(), Some(node.clone()), 0));
            }
        } else {
            // Post-visit.
            if !is_injecting {
                // Check if we want to inject here.
                if should_inject(&node) {
                    is_injecting = true;
                    let new_node: Arc<dyn ExecutionPlan> = Arc::new(Tap::new(node.clone()));
                    injected_plan = Some(new_node);
                    next_parent = parent.map(|p| p.clone());
                    orig_child = Some(node.clone());
                }
            } else {
                let npc = next_parent.clone();
                let occ = orig_child.clone();
                let ipc = injected_plan.clone();
                match (npc, occ, ipc) {
                    (Some(np), Some(oc), Some(ip)) => {
                        if !Arc::ptr_eq(&np, &node) {
                            continue;
                        }

                        let new_children = node
                            .children()
                            .iter()
                            .map(|child| {
                                if Arc::ptr_eq(child, &oc) {
                                    ip.clone()
                                } else {
                                    child.clone()
                                }
                            })
                            .collect::<Vec<Arc<dyn ExecutionPlan>>>();
                        let new_node = node.clone().with_new_children(new_children)?;

                        // Adjust bookkeeping.
                        injected_plan = Some(new_node);
                        orig_child = Some(node);
                        next_parent = parent;
                    }
                    _ => (),
                }
            }
        }
    }

    // None indicates the injection did not occur.
    Ok(injected_plan)
}
