import os

from pathlib import Path
from datetime import datetime, timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from hubmap_operators.common_operators import (
    LogInfoOperator,
    JoinOperator,
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
    SetDatasetProcessingOperator,
    MoveDataOperator,
)

import utils
from utils import (
    get_absolute_workflows,
    get_cwltool_base_cmd,
    get_dataset_uuid,
    get_parent_dataset_uuids_list,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_previous_revision_uuid,
    get_uuid_for_error,
    join_quote_command_str,
    make_send_status_msg_function,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_threads_resource,
    get_preserve_scratch_resource,
    pythonop_get_dataset_state,
    get_local_vm,
)

default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["joel.welling@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("salmon_rnaseq_bulk"),
    "executor_config": {"SlurmExecutor": {"output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                          "cpus-per-task": str(get_threads_resource("salmon_rnsaeq_bulk"))}},
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}


def get_organism_name() -> str:
    return "human"


with HMDAG(
    "salmon_rnaseq_bulk",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("salmon_rnaseq_bulk"),
    },
) as dag:
    pipeline_name = "salmon-rnaseq-bulk"
    cwl_workflows = get_absolute_workflows(
        Path("salmon-rnaseq", "bulk-pipeline.cwl"),
    )

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        data_dir = get_parent_data_dir(**kwargs)

        source_type = ""
        unique_source_types = set()
        for parent_uuid in get_parent_dataset_uuids_list(**kwargs):
            dataset_state = pythonop_get_dataset_state(
                dataset_uuid_callable=lambda **kwargs: parent_uuid, **kwargs
            )
            source_type = dataset_state.get("source_type")
            if source_type == "mixed":
                print("Force failure. Should only be one unique source_type for a dataset.")
            else:
                unique_source_types.add(source_type)

        if len(unique_source_types) > 1:
            print("Force failure. Should only be one unique source_type for a dataset.")
        else:
            source_type = unique_source_types.pop().lower()

        command = [
            *get_cwltool_base_cmd(tmpdir),
            "--outdir",
            tmpdir / "cwl_out",
            "--parallel",
            cwl_workflows[0],
            "--fastq_dir",
            data_dir,
            "--threads",
            get_threads_resource(dag.dag_id),
            "--organism",
            source_type,
        ]

        return join_quote_command_str(command)

    t_build_cmd1 = PythonOperator(
        task_id="build_cmd1",
        python_callable=build_cwltool_cmd1,
        provide_context=True,
    )

    t_pipeline_exec = BashOperator(
        task_id="pipeline_exec",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        {{ti.xcom_pull(task_ids='build_cmd1')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl1 = BranchPythonOperator(
        task_id="maybe_keep_cwl1",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec",
        },
    )

    t_send_create_dataset = PythonOperator(
        task_id="send_create_dataset",
        python_callable=utils.pythonop_send_create_dataset,
        provide_context=True,
        op_kwargs={
            "parent_dataset_uuid_callable": get_parent_dataset_uuids_list,
            "http_conn_id": "ingest_api_connection",
            "previous_revision_uuid_callable": get_previous_revision_uuid,
            "dataset_name_callable": build_dataset_name,
            "pipeline_shorthand": "Salmon",
        },
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
            "mem": "2G"}},
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="one_success",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
        },
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
            "mem": "2G"}},
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=["pipeline_exec", "move_data"],
        cwl_workflows=cwl_workflows,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=send_status_msg,
        provide_context=True,
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
            "mem": "2G"}},
    )

    t_log_info = LogInfoOperator(task_id="log_info",
                                 executor_config={"SlurmExecutor": {
                                     "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                     "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
                                     "mem": "2G"}},)
    t_join = JoinOperator(task_id="join",
                          executor_config={"SlurmExecutor": {
                              "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                              "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
                              "mem": "2G"}},)
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir",
                                           executor_config={"SlurmExecutor": {
                                               "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                               "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
                                               "mem": "2G"}},)
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir",
                                             executor_config={"SlurmExecutor": {
                                                 "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                                 "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
                                                 "mem": "2G"}},)
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing",
                                                            executor_config={"SlurmExecutor": {
                                                                "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                                                "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
                                                                "mem": "2G"}},)
    t_move_data = MoveDataOperator(task_id="move_data",
                                   executor_config={"SlurmExecutor": {
                                       "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                       "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"]),
                                       "mem": "2G"}},)

    (
            t_log_info
            >> t_create_tmpdir
            >> t_send_create_dataset
            >> t_set_dataset_processing
            >> prepare_cwl1
            >> t_build_cmd1
            >> t_pipeline_exec
            >> t_maybe_keep_cwl1
            >> t_move_data
            >> t_send_status
            >> t_join
    )
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
