# Provisioning
## Usage
```py
physical_provisioning = PhysicalProvisioning(monitor, initial_blueprint[, cluster_ids])
if physical_provisioning.should_trigger_replan():
    new_blueprint = ... # Do replanning to make a new blueprint.
    physical_provisioning.update_blueprint(new_blueprint)
```


## Triggers
By default, the metrics in `thresholds.json` are used as triggers. But you can specify additional ones with:
```py
physical_provisioning.change_trigger("aurora", "WriteLatency", lo=1, hi=100)
```
Note the trigger can only work if the metric is read by the monitor.


## Testing Only
To test triggers without actually running a workload, you can override metrics values:
```py
if physical_provisioning.should_trigger_replan({"aurora_WriteLatency_Average": 200}):
    # Do replan.
    pass
```