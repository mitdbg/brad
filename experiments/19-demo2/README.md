# Operating the BRAD Demo

1. Start a new tmux session; you need 3 tmux panes
2. Activate the virtualenv that has BRAD and its dependencies installed
3. Run `./start_brad.sh` to start BRAD. The web interface should be accessible
   on port 7583. Make sure to start the Aurora resource.
4. In another tmux pane, run `./start_workload.sh` to start the workload
5. BRAD relies on system metrics (retrieved from AWS) to do its planning. You
   need to wait at least 3 minutes for the workload metrics to make it to AWS
   before you can start the planner.
6. To shut down the demo, shut down the workload runner first (Ctrl-C). Wait
   until all runners have exited. Then shut down BRAD (Ctrl-C on the other pane).

## Running the editable VDBE demo scenario

- Edit `config/system_config_demo.yml` and switch the `bootstrap_vdbe_path`
  value to `imdb_editable_vdbes.json`.
- Run `./start_brad_editable.sh` to run using the schema designed for demoing the
  editable VDBEs. Make sure to start the Redshift resource as well.

## Important files

- `config/system_config_demo.yml`:
  - BRAD configs for the demo (checked in)
- `config/physical_config_100gb_demo.yml`:
  - Physical configuration values; ensure the cluster IDs refer to actual AWS resources
