# Setup
The cost model directory is adapted from https://github.com/DataManagementLab/zero-shot-cost-estimation, 
containing a lot of legacy file.

Additional packages are needed to use cost model in brad: pytorch, torchvision, dgl, tqdm, optuna

If you are using pip, run:
```angular2html
pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cpu
pip install dgl -f https://data.dgl.ai/wheels/repo.html
pip install tqdm
pip install optuna
```

# Fastforward: 

You can skip the workload generation, execution, and model training. Ask Ziniu for trained models.

Execute cost model inference for Brad:
```angular2html
python run_cost_model.py --infer_brad 
              --infer_brad_sql_file workloads/IMDB/OLAP_queries/all_queries.sql
              --statistics_file ../data/imdb/parsed_queries/statistics_workload_combined.json
              --infer_brad_db_stats_file workloads/cross_db_benchmark/datasets/imdb/database_stats.json
              --infer_brad_model_dir ../data/imdb/evaluation/ 
              --filename_model imdb_1
              --host xxxx
              --port 5439
              --user xxx
              --password 'xxxx'
```
set --infer_brad_runtime_file if necessary. 

# Generate query workload
```angular2html
python run_cost_model.py --generate_workloads --db_name imdb --data_dir ../data/imdb/dataset --workload_dir ../data/imdb/workloads
```

# Execute the queries
## On Aurora:
```angular2html
python run_cost_model.py --run_workload 
              --database aurora
              --db_name imdb
              --query_timeout 200 
              --host xxxx
              --port 5439
              --user xxx
              --password 'xxxx'
              --source ../data/imdb/workloads/complex_workload_10k_s1.sql
              --target ../data/imdb/raw/aurora_complex_workload_10k_s1.json
```

## On redshift:
```angular2html
python run_cost_model.py --run_workload 
              --database redshift
              --db_name imdb
              --query_timeout 200
              --repetitions_per_query 4
              --host xxxx
              --port 5439
              --user xxx
              --password 'xxxx'
              --source ../data/imdb/workloads/complex_workload_10k_s1.sql
              --target ../data/imdb/raw/redshift_complex_workload_10k_s1.json
```

## On athena:
Need to modify the connection string in workloads/cross_db_benchmark/benchmark_tools/athena/database_connection.py
(TODO: make it an argparse input).
```angular2html
python run_cost_model.py --run_workload 
              --database athena
              --db_name imdb
              --query_timeout 200
              --source ../data/imdb/workloads/complex_workload_10k_s1.sql
              --target ../data/imdb/raw/athena_complex_workload_10k_s1.json
```

# Parse the queries

## For Aurora:
Set flag --is_brad since brad has different table names
```angular2html
python run_cost_model.py --database aurora --parse_queries --db_name imdb --include_zero_card --workload_runs ../data/imdb/raw/aurora_IMDB_10k_10_6.json --target ../data/imdb/parsed_queries/ --is_brad
```


Argment dataset if needed (highly recommended for aurora):
```angular2html
python run_cost_model.py --argment_dataset --workload_runs ../data/imdb/parsed_queries/aurora_IMDB_10k_10_6_parsed_queries.json --target ../data/imdb/parsed_queries/
```


## On Redshift
```angular2html
python run_cost_model.py --database redshift --parse_queries --db_name imdb --include_zero_card --workload_runs ../data/imdb/raw/redshift_IMDB_10k_10_6.json --aurora_workload_runs ../data/imdb/raw/aurora_IMDB_10k_10_6.json --target ../data/imdb/parsed_queries/ --is_brad
```

## On Athena
```angular2html
python run_cost_model.py --database athena --parse_queries --db_name imdb --include_zero_card --workload_runs ../data/imdb/raw/athena_IMDB_10k_10_6.json --aurora_workload_runs ../data/imdb/raw/aurora_IMDB_10k_10_6.json --target ../data/imdb/parsed_queries/ --is_brad
```

# Training the cost model


## First gather useful statistics

```angular2html
python run_cost_model.py --gather_feature_statistics --workload_runs ../data/imdb/parsed_queries/aurora_IMDB_10k_10_6_parsed_queries.json --target /home/ziniuw/data/imdb/parsed_queries/statistics_workload_combined.json
```

## Train Aurora cost model

```angular2html
python run_cost_model.py --database aurora --train_model --workload_runs ../data/imdb/parsed_queries/aurora_IMDB_10k_10_6_train.json --test_workload_runs ../data/imdb/parsed_queries/aurora_IMDB_10k_10_6_test.json --statistics_file ../data/imdb/parsed_queries/statistics_workload_combined.json --target ../data/imdb/evaluation/ --hyperparameter_path src/brad/cost_model/setup/tuned_hyperparameters/aurora_tune_est_best_config.json --max_epoch_tuples 100000 --loss_class_name QLoss --device cpu --filename_model imdb_1_aurora --num_workers 16 --seed 0 --save_best
```

## Train Redshift cost model

```angular2html
python run_cost_model.py --database redshift --train_model --workload_runs ../data/imdb/parsed_queries/redshift_IMDB_10k_10_6_train.json --test_workload_runs ../data/imdb/parsed_queries/redshift_IMDB_10k_10_6_test.json --statistics_file ../data/imdb/parsed_queries/statistics_workload_combined.json --target ../data/imdb/evaluation/ --hyperparameter_path src/brad/cost_model/setup/tuned_hyperparameters/redshift_tune_est_best_config.json --max_epoch_tuples 100000 --loss_class_name QLoss --device cpu --filename_model imdb_1_redshift --num_workers 16 --seed 0 --save_best
```

## Train Athena cost model

```angular2html
python run_cost_model.py --database athena --train_model --workload_runs ../data/imdb/parsed_queries/athena_IMDB_10k_10_6_train.json --test_workload_runs ../data/imdb/parsed_queries/athena_IMDB_10k_10_6_test.json --statistics_file ../data/imdb/parsed_queries/statistics_workload_combined.json --target ../data/imdb/evaluation/ --hyperparameter_path src/brad/cost_model/setup/tuned_hyperparameters/athena_tune_est_best_config.json --max_epoch_tuples 100000 --loss_class_name QLoss --device cpu --filename_model imdb_1_athena --num_workers 16 --seed 0 --save_best
```


