#!/bin/bash

# What variables do we need?
# Define CODCC machines
#codcc_machines=("c0" "c1" "c2" "c3" "c4" "c5")
codcc_machines=()
repo_env="dev"
repo_dir="/opt/repositories/$(hostname -s)-$repo_env/ingest-pipeline"
python_version=3.9

regenerate_env=false

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
for machine in "${codcc_machines[@]}"; do
       	# Rsync repo to machine
        rsync -a --exclude "src/ingest-pipeline/airflow/logs" $repo_dir/ $machine:$repo_dir

       	# If flag set, run the conda environment regenerations
        if $regenerate_env ; then
                ssh $machine "/usr/local/bin/update_hubmap.sh $repo_dir $repo_env $python_version"
        fi
done
