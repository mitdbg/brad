# Operating the BRAD Demo

## Scenario 1 (Predictions)

1. Start a new tmux session; you need 2 tmux panes
2. Activate the virtualenv that has BRAD and its dependencies installed
3. Run `./start_brad.sh` to start BRAD. The web interface should be accessible
   on port 7583. Make sure to start the Aurora resource.
4. In another tmux pane, run `./start_workload.sh` to start the workload
5. BRAD relies on system metrics (retrieved from AWS) to do its planning. You
   need to wait at least 3 minutes for the workload metrics to make it to AWS
   before you can start the planner.
6. To shut down the demo, shut down the workload runner first (Ctrl-C). Wait
   until all runners have exited. Then shut down BRAD (Ctrl-C on the other pane).

## Scenario 2 (Editable VDBEs)

1. Run `./start_brad_editable.sh` to run using the schema designed for demoing the
   editable VDBEs. Make sure to start the Redshift resource as well.
2. The web interface should be accessible on port 7683.

## Scenario 3 (AWS Glue ETLs)

1. Run `./start_brad_etl.sh` to run using the schema designed for demoing the
   external ETL scenario. Make sure to start Redshift and Aurora.
2. The web interface should be accessible on port 7783.

Use `set_up_etl_blueprint.py` to transition to the blueprint for this scenario.

## Important files

- `config/system_config_demo_s{1,2,3}.yml`:
  - BRAD configs for the demo scenarios (checked in)
- `config/physical_config_100gb_demo.yml`:
  - Physical configuration values; ensure the cluster IDs refer to actual AWS resources
