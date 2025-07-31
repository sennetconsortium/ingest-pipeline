#!/bin/bash

# What variables do we need?
# Define CODCC machines
# codcc_machines=("c0" "c1" "c2" "c3" "c4" "c5" "g0" "g1")
codcc_machines=()

priority_list=("-prod" "-test" "-dev")
base_name="airflow"
file_dir="/etc/sysconfig"

regenerate_env=false

function set_main_environment(){
  for ending in "${priority_list[@]}"; do
    file="$file_dir/$base_name$ending"
    if [ -f "$file" ]; then
        echo "Selected file: $file"
        source $file
        repo_env="$HUBMAP_INSTANCE"
        python_version="$HUBMAP_PYTHON_VERSION"
        repo_dir="/opt/repositories/$(hostname -s)-$repo_env/ingest-pipeline"
        break
    fi
done
}

while getopts ":g" opt; do
  case $opt in
    g)
      regenerate_env=true
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      ;;
  esac
done

set -x

set_main_environment

if [ -z "$repo_env" ]; then
    exit "The environment variable HUBMAP_INSTANCE is not set."
fi

for machine in "${codcc_machines[@]}"; do
       	# Rsync repo to machine
        rsync -a --exclude "src/ingest-pipeline/airflow/logs" $repo_dir/ $machine:$repo_dir

       	# If flag set, run the conda environment regenerations
        if $regenerate_env ; then
                ssh $machine "/usr/local/bin/update_moonshot.sh $repo_dir $repo_env $python_version"
        fi
done
