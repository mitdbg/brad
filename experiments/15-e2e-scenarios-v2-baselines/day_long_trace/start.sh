script_loc=$(cd $(dirname $0) && pwd -P)
bash $script_loc/run_workload.sh

# # Untar into trace folder
# filename="daylong_trace.tar.gz"
# folder="trace"
# tar -xzf $filename -C $folder