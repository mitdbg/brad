# Dataset preprocessing pipeline

- Gather data using the existing `run_cost_model.py`
- Run query parsing using the existing `run_cost_model.py` to get the "parsed dataset"
- (If needed) Use `merge_collected.py` to merge the parsed results (e.g., if you
  did multiple data collection passes)
- Use `unify.py` to group up the collected data into one "standard dataset"
- Use `dataset_selection.py` to do a train-test split (note that this script
  needs manual modification)
- If this is a key dataset used in the evaluation, commit it under `workloads`
  (see `IMDB_100GB`).
- Use `prepare_datasets.sh` to massage the data into the right format for
  running with the existing GNN model training script
