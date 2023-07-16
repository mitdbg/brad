#! /bin/bash

# Need to run this collection across 4 instances in parallel to complete in a
# reasonable amount of time.
if [ -z $4 ]; then
  >&2 echo "Usage: $0 <instance> <config_path> <group_num> <cluster_identifier>"
  >&2 echo "The config path should be relative to the redshift/ subdirectory."
  exit 1
fi

export BRAD_SCHEMA="imdb"
export BRAD_CONFIG=$2

instance=$1
group=$3
cluster_identifier=$4

function run_warm_up() {
  >&2 echo "Running warm up..."
  pushd redshift
  python3 -m brad.calibration.measure_load --run-warmup --engine redshift --query-file ../query_bank.sql
  popd
}

function sync_redshift_resize() {
  raw_instance=$1
  target_instance_type=${raw_instance//_/.}
  target_node_count=$2

  # Try an elastic resize first.
  >&2 echo "Resizing Redshift cluster to $target_instance_type with $target_node_count nodes (attempt elastic)"
  aws redshift resize-cluster --cluster-identifier "$cluster_identifier" --cluster-type multi-node --node-type "$target_instance_type" --number-of-nodes "$target_node_count" --no-classic --region us-east-1
  result=$?

  # Resize Redshift cluster
  if [ $result -ne 0 ]; then
    >&2 echo "Classic resizing Redshift cluster to $target_instance_type with $target_node_count nodes"
    aws redshift modify-cluster --cluster-identifier "$cluster_identifier" --node-type "$target_instance_type" --number-of-nodes "$target_node_count"
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

>&2 echo "$instance 1x ($group of 2)"
sync_redshift_resize $instance 1
run_warm_up
cond run "//redshift:${instance}-1-${group}-of-2"

>&2 echo "$instance 2x ($group of 2)"
sync_redshift_resize $instance 2
run_warm_up
cond run "//redshift:${instance}-2-${group}-of-2"

>&2 echo "$instance 4x ($group of 2)"
sync_redshift_resize $instance 4
run_warm_up
cond run "//redshift:${instance}-4-${group}-of-2"
