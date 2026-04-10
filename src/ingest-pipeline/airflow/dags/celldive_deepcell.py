import os

from datetime import datetime, timedelta
from pathlib import Path

from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator, PythonOperator
from status_change.callbacks.failure_callback import FailureCallback


import utils
from utils import (
    downstream_workflow_iter,
    get_dataset_uuid,
    get_absolute_workflow,
    get_parent_data_dir,
    build_dataset_name as inner_build_dataset_name,
    get_uuid_for_error,
    join_quote_command_str,
    get_tmp_dir_path,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_cwl_cmd_from_workflows,
    get_threads_resource,
    get_local_vm,
)

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    LogInfoOperator,
)
from hubmap_operators.flex_multi_dag_run import FlexMultiDagRunOperator

from airflow.configuration import conf as airflow_conf

from extra_utils import build_tag_containers


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
    "queue": get_queue_resource("celldive_deepcell_segmentation"),
    "executor_config": {"SlurmExecutor": {"output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                          "cpus-per-task": str(get_threads_resource("celldive_deepcell_segmentation")),}},
    "on_failure_callback": FailureCallback(__name__, get_uuid_for_error),
}

with HMDAG(
    "celldive_deepcell_segmentation",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("celldive_deepcell_segmentation"),
    },
) as dag:
    pipeline_name = "celldive-pipeline"
    workflow_version = "1.0.0"
    workflow_description = "The CellDive pipeline performs segments nuclei and cells using Cytokit, and performs spatial analysis of expression data using SPRM, which computes various measures of analyte intensity per cell, performs clustering based on expression and other data, and computes markers for each cluster."

    cwl_workflows = [
        {
            "workflow_path": str(
                get_absolute_workflow(Path("phenocycler-pipeline", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(Path("sprm", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("create-vis-symlink-archive", "pipeline.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(get_absolute_workflow(Path("ome-tiff-pyramid", "pipeline.cwl"))),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "ome-tiff-offsets.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "sprm-to-json.cwl"))
            ),
            "documentation_url": "",
        },
        {
            "workflow_path": str(
                get_absolute_workflow(Path("portal-containers", "sprm-to-anndata.cwl"))
            ),
            "documentation_url": "",
        },
    ]

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    @task(task_id="prepare_cwl_segmentation")
    def prepare_cwl_cmd1(**kwargs):
        if kwargs["dag_run"].conf.get("dryrun"):
            cwl_path = Path(cwl_workflows[0]["workflow_path"]).parent
            return build_tag_containers(cwl_path)
        else:
            return "No Container build required"

    prepare_cwl_segmentation = prepare_cwl_cmd1()

    def build_cwltool_cwl_segmentation(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("mpdir: ", tmpdir)
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        workflow = cwl_workflows[0]
        meta_yml_path = str(Path(workflow["workflow_path"]).parent / "meta.yaml")

        input_parameters = [
            {"parameter_name": "--gpus", "value": "all"},
            {"parameter_name": "--segmentation_method", "value": "deepcell"},
            {"parameter_name": "--data_dir", "value": str(data_dir)},
            {"parameter_name": "--invert_geojson_mask", "value": ""},
        ]

        command = get_cwl_cmd_from_workflows(
            cwl_workflows, 0, input_parameters, tmpdir, kwargs["ti"]
        )

        return join_quote_command_str(command)

    t_build_cwl_segmentation = PythonOperator(
        task_id="build_cwl_segmentation",
        python_callable=build_cwltool_cwl_segmentation,
        provide_context=True,
    )

    t_pipeline_exec_cwl_segmentation = BashOperator(
        task_id="pipeline_exec_cwl_segmentation",
        bash_command=""" \
        tmp_dir={{tmp_dir_path(run_id)}} ; \
        mkdir -p ${tmp_dir}/cwl_out ; \
        {{ti.xcom_pull(task_ids='build_cwl_segmentation')}} > $tmp_dir/session.log 2>&1 ; \
        echo $?
        """,
    )

    t_maybe_keep_cwl_segmentation = BranchPythonOperator(
        task_id="maybe_keep_cwl_segmentation",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_stellar_pre_convert",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_segmentation",
        },
    )

    t_set_dataset_error = PythonOperator(
        task_id="set_dataset_error",
        python_callable=utils.pythonop_set_dataset_state,
        provide_context=True,
        trigger_rule="all_done",
        op_kwargs={
            "dataset_uuid_callable": get_dataset_uuid,
            "ds_state": "Error",
            "message": "An error occurred in {}".format(pipeline_name),
            "pipeline_name": pipeline_name
        },
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_AIRFLOW_CONNECTION"]),
            "mem": "2G"}},
    )

    t_log_info = LogInfoOperator(task_id="log_info")
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir")

    def trigger_sprm(**kwargs):
        collection_type = kwargs.get("collection_type", "")
        assay_type = kwargs.get("assay_type", "")
        payload = {
            "tmp_dir": get_tmp_dir_path(kwargs["run_id"]),
            "parent_submission_id": kwargs["dag_run"].conf.get("parent_submission_id"),
            "parent_lz_path": kwargs["dag_run"].conf.get("parent_lz_path"),
            "previous_version_uuid": kwargs["dag_run"].conf.get("previous_version_uuid"),
            "metadata": kwargs["dag_run"].conf.get("metadata"),
            "crypt_auth_tok": kwargs["dag_run"].conf.get("crypt_auth_tok"),
            "workflows": kwargs["ti"].xcom_pull(
                task_ids="build_cwl_segmentation", key="cwl_workflows"
            ),
        }
        print(
            f"Collection_type: {collection_type} with assay_type {assay_type} and payload: {payload}",
        )
        for next_dag in downstream_workflow_iter(collection_type, assay_type):
            yield next_dag, payload

    t_trigger_sprm = FlexMultiDagRunOperator(
        task_id="trigger_sprm",
        dag=dag,
        trigger_dag_id="celldive_segmentation",
        python_callable=trigger_sprm,
        op_kwargs={"collection_type": "celldive_sprm", "assay_type": "celldive"},
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_AIRFLOW_CONNECTION"]),
            "mem": "2G"}},
    )

    (
        t_log_info
        >> t_create_tmpdir

        >> prepare_cwl_segmentation
        >> t_build_cwl_segmentation
        >> t_pipeline_exec_cwl_segmentation
        >> t_maybe_keep_cwl_segmentation
        >> t_trigger_sprm

    )
    t_maybe_keep_cwl_segmentation >> t_set_dataset_error
