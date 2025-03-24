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
    get_preserve_scratch_resource,
    get_local_vm, get_threads_resource,
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
    "queue": get_queue_resource("geomx"),
    "executor_config": {"SlurmExecutor": {"output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                          "cpus-per-task": str(get_threads_resource("geomx"))}},
    "on_failure_callback": utils.create_dataset_state_error_callback(get_uuid_for_error),
}


with HMDAG(
    "geomx",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("geomx"),
    },
) as dag:

    pipeline_name = "geomx"
    cwl_workflows = get_absolute_workflows(
        Path("geomx-pipeline", "pipeline.cwl"),
        Path("ome-tiff-pyramid", "pipeline.cwl"),
        Path("portal-containers", "ome-tiff-offsets.cwl"),
        Path("portal-containers", "ome-tiff-segments.cwl"),
        Path("portal-containers", "ome-tiff-metadata.cwl"),
    )

    def build_dataset_name(**kwargs):
        return inner_build_dataset_name(dag.dag_id, pipeline_name, **kwargs)

    prepare_cwl1 = DummyOperator(task_id="prepare_cwl1")

    def build_cwltool_cmd1(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        data_dir = get_parent_data_dir(**kwargs)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            "--relax-path-checks",
            "--outdir",
            tmpdir / "cwl_out",
            "--parallel",
            cwl_workflows[0],
            "--data_dir",
            data_dir,
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
            "next_op": "prepare_cwl2",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec",
        },
    )

    prepare_cwl2 = DummyOperator(task_id="prepare_cwl2")

    def build_cwltool_cmd2(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # data directory is the stitched images, which are found in tmpdir
        data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", data_dir)

        # this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[1],
            "--processes",
            get_threads_resource(dag.dag_id),
            "--ometiff_directory",
            data_dir / "lab_processed/images/",
        ]
        return join_quote_command_str(command)

    t_build_cmd2 = PythonOperator(
        task_id="build_cmd2",
        python_callable=build_cwltool_cmd2,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid_base = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid_base",
        bash_command=""" \
                tmp_dir={{tmp_dir_path(run_id)}} ; \
                mkdir -p ${tmp_dir}/cwl_out ; \
                cd ${tmp_dir}/cwl_out ; \
                {{ti.xcom_pull(task_ids='build_cmd2')}} >> $tmp_dir/session.log 2>&1 ; \
                echo $?
                """,
    )

    t_maybe_keep_cwl2 = BranchPythonOperator(
        task_id="maybe_keep_cwl2",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl3",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid_base",
        },
    )

    prepare_cwl3 = DummyOperator(task_id="prepare_cwl3")

    def build_cwltool_cmd3(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[2],
            "--input_dir",
            data_dir / "ometiff-pyramids",
        ]

        return join_quote_command_str(command)

    t_build_cmd3 = PythonOperator(
        task_id="build_cmd3",
        python_callable=build_cwltool_cmd3,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_offsets_base = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_offsets_base",
        bash_command=""" \
                tmp_dir={{tmp_dir_path(run_id)}} ; \
                cd ${tmp_dir}/cwl_out ; \
                {{ti.xcom_pull(task_ids='build_cmd3')}} >> ${tmp_dir}/session.log 2>&1 ; \
                echo $?
                """,
    )

    t_maybe_keep_cwl3 = BranchPythonOperator(
        task_id="maybe_keep_cwl3",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl4",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets_base",
        },
    )

    prepare_cwl4 = DummyOperator(task_id="prepare_cwl4")

    def build_cwltool_cmd4(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[3],
            "--input_dir",
            data_dir / "ometiff-pyramids",
        ]

        return join_quote_command_str(command)


    t_build_cmd4 = PythonOperator(
        task_id="build_cmd4",
        python_callable=build_cwltool_cmd4,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_segments_base = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_segments_base",
        bash_command=""" \
                    tmp_dir={{tmp_dir_path(run_id)}} ; \
                    cd ${tmp_dir}/cwl_out ; \
                    {{ti.xcom_pull(task_ids='build_cmd4')}} >> ${tmp_dir}/session.log 2>&1 ; \
                    echo $?
                    """,
    )

    t_maybe_keep_cwl4 = BranchPythonOperator(
        task_id="maybe_keep_cwl4",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl5",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_segments_base",
        },
    )

    prepare_cwl5 = DummyOperator(task_id="prepare_cwl5")

    def build_cwltool_cmd5(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[4],
            "--input_dir",
            data_dir / "ometiff-pyramids",
        ]

        return join_quote_command_str(command)


    t_build_cmd5 = PythonOperator(
        task_id="build_cmd5",
        python_callable=build_cwltool_cmd5,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_metadata_base = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_metadata_base",
        bash_command=""" \
                    tmp_dir={{tmp_dir_path(run_id)}} ; \
                    cd ${tmp_dir}/cwl_out ; \
                    {{ti.xcom_pull(task_ids='build_cmd5')}} >> ${tmp_dir}/session.log 2>&1 ; \
                    echo $?
                    """,
    )

    t_maybe_keep_cwl5 = BranchPythonOperator(
        task_id="maybe_keep_cwl5",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl6",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_metadata_base",
        },
    )

    prepare_cwl6 = DummyOperator(task_id="prepare_cwl6")

    def build_cwltool_cmd6(**kwargs):
        run_id = kwargs["run_id"]

        # tmpdir is temp directory in /hubmap-tmp
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)

        # data directory is the stitched images, which are found in tmpdir
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("data_dir: ", parent_data_dir)

        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        # this is the call to the CWL
        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[1],
            "--processes",
            get_threads_resource(dag.dag_id),
            "--ometiff_directory",
            data_dir / "output_ome_segments",
        ]
        return join_quote_command_str(command)

    t_build_cmd6 = PythonOperator(
        task_id="build_cmd6",
        python_callable=build_cwltool_cmd6,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_pyramid_segments = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_pyramid_segments",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            cd ${tmp_dir}/cwl_out ; \
            {{ti.xcom_pull(task_ids='build_cmd6')}} >> $tmp_dir/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl6 = BranchPythonOperator(
        task_id="maybe_keep_cwl6",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl7",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_pyramid_segments",
        },
    )

    prepare_cwl7 = DummyOperator(task_id="prepare_cwl7")

    def build_cwltool_cmd7(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[2],
            "--input_dir",
            data_dir / "output_ome_segments",
        ]

        return join_quote_command_str(command)

    t_build_cmd7 = PythonOperator(
        task_id="build_cmd7",
        python_callable=build_cwltool_cmd7,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_offsets_segments = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_offsets_segments",
        bash_command=""" \
            tmp_dir={{tmp_dir_path(run_id)}} ; \
            cd ${tmp_dir}/cwl_out ; \
            {{ti.xcom_pull(task_ids='build_cmd7')}} >> ${tmp_dir}/session.log 2>&1 ; \
            echo $?
            """,
    )

    t_maybe_keep_cwl7 = BranchPythonOperator(
        task_id="maybe_keep_cwl7",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "prepare_cwl8",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_offsets_segments",
        },
    )

    prepare_cwl8 = DummyOperator(task_id="prepare_cwl8")

    def build_cwltool_cmd8(**kwargs):
        run_id = kwargs["run_id"]
        tmpdir = get_tmp_dir_path(run_id)
        print("tmpdir: ", tmpdir)
        parent_data_dir = get_parent_data_dir(**kwargs)
        print("parent_data_dir: ", parent_data_dir)
        data_dir = tmpdir / "cwl_out"
        print("data_dir: ", data_dir)

        command = [
            *get_cwltool_base_cmd(tmpdir),
            cwl_workflows[4],
            "--input_dir",
            data_dir / "output_ome_segments",
        ]

        return join_quote_command_str(command)


    t_build_cmd8 = PythonOperator(
        task_id="build_cmd8",
        python_callable=build_cwltool_cmd8,
        provide_context=True,
    )

    t_pipeline_exec_cwl_ome_tiff_metadata_segments = BashOperator(
        task_id="pipeline_exec_cwl_ome_tiff_metadata_segments",
        bash_command=""" \
                tmp_dir={{tmp_dir_path(run_id)}} ; \
                cd ${tmp_dir}/cwl_out ; \
                {{ti.xcom_pull(task_ids='build_cmd8')}} >> ${tmp_dir}/session.log 2>&1 ; \
                echo $?
                """,
    )

    t_maybe_keep_cwl8 = BranchPythonOperator(
        task_id="maybe_keep_cwl8",
        python_callable=utils.pythonop_maybe_keep,
        provide_context=True,
        op_kwargs={
            "next_op": "move_data",
            "bail_op": "set_dataset_error",
            "test_op": "pipeline_exec_cwl_ome_tiff_metadata_segments",
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
            "pipeline_shorthand": "AnnData",
        },
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},
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
        },
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},
    )

    send_status_msg = make_send_status_msg_function(
        dag_file=__file__,
        retcode_ops=["pipeline_exec",
                     "pipeline_exec_cwl_ome_tiff_pyramid_base",
                     "pipeline_exec_cwl_ome_tiff_offsets_base",
                     "pipeline_exec_cwl_ome_tiff_segments_base",
                     "pipeline_exec_cwl_ome_tiff_metadata_base",
                     "pipeline_exec_cwl_ome_tiff_pyramid_segments",
                     "pipeline_exec_cwl_ome_tiff_offsets_segments",
                     "pipeline_exec_cwl_ome_tiff_metadata_segments",
                     "move_data"],
        cwl_workflows=cwl_workflows,
    )

    t_send_status = PythonOperator(
        task_id="send_status_msg",
        python_callable=send_status_msg,
        provide_context=True,
        executor_config={"SlurmExecutor": {
            "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
            "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},
    )

    t_log_info = LogInfoOperator(task_id="log_info",
                                 executor_config={"SlurmExecutor": {
                                     "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                     "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},)
    t_join = JoinOperator(task_id="join",
                          executor_config={"SlurmExecutor": {
                              "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                              "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},)
    t_create_tmpdir = CreateTmpDirOperator(task_id="create_tmpdir",
                                           executor_config={"SlurmExecutor": {
                                               "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                               "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},)
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_tmpdir",
                                             executor_config={"SlurmExecutor": {
                                                 "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                                 "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},)
    t_set_dataset_processing = SetDatasetProcessingOperator(task_id="set_dataset_processing",
                                                            executor_config={"SlurmExecutor": {
                                                                "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                                                "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},)
    t_move_data = MoveDataOperator(task_id="move_data",
                                   executor_config={"SlurmExecutor": {
                                       "output": "/home/codcc/airflow-logs/slurm/%x_%N_%j.out",
                                       "nodelist": get_local_vm(os.environ["AIRFLOW_CONN_INGEST_API_CONNECTION"])}},)
    (
        t_log_info
        >> t_create_tmpdir
        >> t_send_create_dataset
        >> t_set_dataset_processing
        >> prepare_cwl1
        >> t_build_cmd1
        >> t_pipeline_exec
        >> t_maybe_keep_cwl1
        >> prepare_cwl2
        >> t_build_cmd2
        >> t_pipeline_exec_cwl_ome_tiff_pyramid_base
        >> t_maybe_keep_cwl2
        >> prepare_cwl3
        >> t_build_cmd3
        >> t_pipeline_exec_cwl_ome_tiff_offsets_base
        >> t_maybe_keep_cwl3
        >> prepare_cwl4
        >> t_build_cmd4
        >> t_pipeline_exec_cwl_ome_tiff_segments_base
        >> t_maybe_keep_cwl4
        >> prepare_cwl5
        >> t_build_cmd5
        >> t_pipeline_exec_cwl_ome_tiff_metadata_base
        >> t_maybe_keep_cwl5
        >> prepare_cwl6
        >> t_build_cmd6
        >> t_pipeline_exec_cwl_ome_tiff_pyramid_segments
        >> t_maybe_keep_cwl6
        >> prepare_cwl7
        >> t_build_cmd7
        >> t_pipeline_exec_cwl_ome_tiff_offsets_segments
        >> t_maybe_keep_cwl7
        >> prepare_cwl8
        >> t_build_cmd8
        >> t_pipeline_exec_cwl_ome_tiff_metadata_segments
        >> t_maybe_keep_cwl8
        >> t_move_data
        >> t_send_status
        >> t_join
    )
    t_maybe_keep_cwl1 >> t_set_dataset_error
    t_maybe_keep_cwl2 >> t_set_dataset_error
    t_maybe_keep_cwl3 >> t_set_dataset_error
    t_maybe_keep_cwl4 >> t_set_dataset_error
    t_maybe_keep_cwl5 >> t_set_dataset_error
    t_maybe_keep_cwl6 >> t_set_dataset_error
    t_maybe_keep_cwl7 >> t_set_dataset_error
    t_maybe_keep_cwl8 >> t_set_dataset_error
    t_set_dataset_error >> t_join
    t_join >> t_cleanup_tmpdir
