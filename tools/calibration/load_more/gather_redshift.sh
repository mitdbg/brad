#! /bin/bash

if [ -z $3 ]; then
  >&2 echo "Usage: $0 <config_path> <cluster id> <rank (1 or 2)>"
  >&2 echo "The config path should be relative to the redshift/ subdirectory."
  exit 1
fi

export BRAD_CONFIG=$1
cluster_identifier=$2
rank=$3

export BRAD_SCHEMA="imdb_extended_100g"

function run_warm_up() {
  >&2 echo "Running warm up..."
  pushd redshift
  python3 -m brad.calibration.measure_load --run-warmup --engine redshift --query-file ../../../../tools/calibration/load_more/query_bank.sql
  popd
}

function sync_redshift_resize() {
  raw_instance=$1
  target_instance_type=${raw_instance//_/.}
  target_node_count=$2

  # Try an elastic resize first.
  >&2 echo "Resizing Redshift cluster to $target_instance_type with $target_node_count nodes (attempt elastic)"
  aws redshift resize-cluster --cluster-identifier "$cluster_identifier" --cluster-type multi-node --node-type "$target_instance_type" --number-of-nodes "$target_node_count" --no-classic --region us-east-1 > /dev/null
  result=$?

  # Resize Redshift cluster
  if [ $result -ne 0 ]; then
    >&2 echo "Classic resizing Redshift cluster to $target_instance_type with $target_node_count nodes"
    aws redshift modify-cluster --cluster-identifier "$cluster_identifier" --node-type "$target_instance_type" --number-of-nodes "$target_node_count" > /dev/null
  fi

  sleep 60

  # Wait for resize to complete
  while true; do
      cluster_status=$(aws redshift describe-clusters --cluster-identifier "$cluster_identifier" --query 'Clusters[0].ClusterStatus' --output text)
      if [[ $cluster_status == "available" ]]; then
          break
      fi
      >&2 echo "Waiting for resize to complete..."
      sleep 10
  done
}

function run_cfg() {
  instance_type=$1
  num_nodes=$2

  >&2 echo "$instance_type $num_nodes"
  sync_redshift_resize $instance_type $num_nodes
  >&2 echo "Warming up..."
  run_warm_up
  >&2 echo "Running..."
  cond run "//redshift:${instance_type}-${num_nodes}-${rank}-of-2"
}

>&2 echo "Starting as $rank"
>&2 echo "Running $cluster_identifier"
>&2 echo "Config $BRAD_CONFIG"
>&2 echo "Cluster id $cluster_identifier"
sleep 10

run_cfg "dc2_large" 2
run_cfg "dc2_large" 4
run_cfg "dc2_large" 8
run_cfg "dc2_large" 16
run_cfg "ra3_xlplus" 2
run_cfg "ra3_xlplus" 4
run_cfg "ra3_xlplus" 8
run_cfg "ra3_4xlarge" 8
run_cfg "ra3_4xlarge" 4
run_cfg "ra3_4xlarge" 2

sleep 60

>&2 echo "Done. Pausing $cluster_identifier..."
aws redshift pause-cluster --cluster-identifier "$cluster_identifier"
