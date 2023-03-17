import sys

import json
import traceback
from pathlib import Path
from datetime import datetime, timedelta
from pprint import pprint

from airflow.configuration import conf as airflow_conf
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.utils.email import send_email

from hubmap_operators.common_operators import (
    CreateTmpDirOperator,
    CleanupTmpDirOperator,
)

from utils import (
    encrypt_tok,
    get_tmp_dir_path,
    get_auth_tok,
    pythonop_get_dataset_state,
    HMDAG,
    get_queue_resource,
    get_preserve_scratch_resource,
    get_threads_resource,
)

sys.path.append(airflow_conf.as_dict()["connections"]["SRC_PATH"].strip("'").strip('"'))
from submodules import (
    ingest_validation_tools_upload,  # noqa E402
    ingest_validation_tools_error_report,
    ingest_validation_tests,
)

sys.path.pop()

"""
TODO
- troubleshoot dag_run
- exception does not capture exception name yuck, is there an elegant way to find it
  or do I need to crawl through traceback
- fix validation message being pushed in failure_email_function
- identify different types of errors and email creator for subset
    - in the case of ingest validation:
        - if [PreflightError, ValidatorError, DirectoryValidationErrors, FileNotFoundError] email submitter
    - probably an add-on to regular process of emailing data curators;
      would want curator to know it was sent to submitted though
    - retrieve submitter data from xcom and construct message using other xcom data
        - create templates
- find proper generalizable home for all of this
- type hinting
"""


def email_submitter(context):
    created_by_user_email = context.get("task_instance").xcom_pull(key="created_by_user_email")
    subject = None
    msg = None
    send_email(to=[created_by_user_email], subject=subject, html_content=msg)


def failure_email_function(context):
    # traceback logic borrowed from https://stackoverflow.com/questions/51822029/get-exception-details-on-airflow-on-failure-callback-context
    # dag_run is not working
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    formatted_exception = "".join(
        traceback.format_exception(
            etype=type(exception), value=exception, tb=exception.__traceback__
        )
    ).strip()
    subject = f"DAG {context.get('dag')} failed at task {context.get('task')}"
    msg = f"""DAG run: {dag_run}
            Error: {exception}
            Traceback: {formatted_exception}"""
    send_email(to=["gesina@psc.edu"], subject=subject, html_content=msg)
    # if exception in [PreflightError, ValidatorError, DirectoryValidationErrors, FileNotFoundError]
    # email_submitter(context)

    """
    It looks like a failure callback will need to manually set the
    dataset status. That's what this does.

    Recycling the send_status task would be better but
    it requires all upstream tasks to succeed.
    This would need to be generalized.
    """
    uuid = context.get("task_instance").xcom_pull(key="uuid")
    crypt_auth_tok = context.get("crypt_auth_tok")
    auth_tok = get_auth_tok(**{"crypt_auth_tok": crypt_auth_tok})
    endpoint = f"/entities/{uuid}"
    headers = {
        "authorization": "Bearer " + auth_tok,
        "X-Hubmap-Application": "ingest-pipeline",
        "content-type": "application/json",
    }
    extra_options = []
    http_conn_id = "entity_api_connection"
    http_hook = HttpHook("PUT", http_conn_id=http_conn_id)
    data = {"status": "Invalid", "validation_message": formatted_exception}
    print("data: ")
    pprint(data)
    response = http_hook.run(
        endpoint,
        json.dumps(data),
        headers,
        extra_options,
    )


# Following are defaults which can be overridden later on
default_args = {
    "owner": "hubmap",
    "depends_on_past": False,
    "start_date": datetime(2019, 1, 1),
    "email": ["gesina@psc.edu"],
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": failure_email_function,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "xcom_push": True,
    "queue": get_queue_resource("validate_upload"),
}

with HMDAG(
    "validate_upload",
    schedule_interval=None,
    is_paused_upon_creation=False,
    default_args=default_args,
    user_defined_macros={
        "tmp_dir_path": get_tmp_dir_path,
        "preserve_scratch": get_preserve_scratch_resource("validate_upload"),
    },
) as dag:

    def find_uuid(**kwargs):
        uuid = kwargs["dag_run"].conf["uuid"]

        def my_callable(**kwargs):
            return uuid

        ds_rslt = pythonop_get_dataset_state(dataset_uuid_callable=my_callable, **kwargs)
        if not ds_rslt:
            raise AirflowException(f"Invalid uuid/doi for group: {uuid}")
        print("ds_rslt:")
        pprint(ds_rslt)

        for key in ["entity_type", "status", "uuid", "data_types", "local_directory_full_path"]:
            assert key in ds_rslt, f"Dataset status for {uuid} has no {key}"

        if ds_rslt["entity_type"] != "Upload":
            raise AirflowException(f"{uuid} is not an Upload")
        if ds_rslt["status"] not in ["New", "Submitted", "Invalid", "Processing"]:
            raise AirflowException(
                f"status of Upload {uuid} is not New, Submitted, Invalid, or Processing"
            )

        lz_path = ds_rslt["local_directory_full_path"]
        uuid = ds_rslt["uuid"]  # 'uuid' may  actually be a DOI
        print(f"Finished uuid {uuid}")
        print(f"lz path: {lz_path}")
        kwargs["ti"].xcom_push(key="lz_path", value=lz_path)
        kwargs["ti"].xcom_push(key="uuid", value=uuid)

    t_find_uuid = PythonOperator(
        task_id="find_uuid",
        python_callable=find_uuid,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            )
        },
    )

    def run_validation(**kwargs):
        # Force an error to test email
        # raise Exception("test")
        lz_path = kwargs["ti"].xcom_pull(key="lz_path")
        uuid = kwargs["ti"].xcom_pull(key="uuid")
        plugin_path = [path for path in ingest_validation_tests.__path__][0]

        ignore_globs = [uuid, "extras", "*metadata.tsv", "validation_report.txt"]
        #
        # Uncomment offline=True below to avoid validating orcid_id URLs &etc
        #
        upload = ingest_validation_tools_upload.Upload(
            directory_path=Path(lz_path),
            dataset_ignore_globs=ignore_globs,
            upload_ignore_globs="*",
            plugin_directory=plugin_path,
            # offline=True,  # noqa E265
            add_notes=False,
            extra_parameters={
                "coreuse": get_threads_resource("validate_upload", "run_validation")
            },
        )
        # Scan reports an error result
        report = ingest_validation_tools_error_report.ErrorReport(
            errors=upload.get_errors(plugin_kwargs=kwargs), info=upload.get_info()
        )
        validation_file_path = Path(get_tmp_dir_path(kwargs["run_id"])) / "validation_report.txt"
        with open(validation_file_path, "w") as f:
            f.write(report.as_text())
        kwargs["ti"].xcom_push(key="validation_file_path", value=str(validation_file_path))

    t_run_validation = PythonOperator(
        task_id="run_validation",
        python_callable=run_validation,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            )
        },
    )

    def send_status_msg(**kwargs):
        validation_file_path = Path(kwargs["ti"].xcom_pull(key="validation_file_path"))
        uuid = kwargs["ti"].xcom_pull(key="uuid")
        endpoint = f"/entities/{uuid}"
        headers = {
            "authorization": "Bearer " + get_auth_tok(**kwargs),
            "X-Hubmap-Application": "ingest-pipeline",
            "content-type": "application/json",
        }
        extra_options = []
        http_conn_id = "entity_api_connection"
        http_hook = HttpHook("PUT", http_conn_id=http_conn_id)
        with open(validation_file_path) as f:
            report_txt = f.read()
        if report_txt.startswith("No errors!"):
            data = {
                "status": "Valid",
            }
        else:
            data = {"status": "Invalid", "validation_message": report_txt}
        print("data: ")
        pprint(data)
        response = http_hook.run(
            endpoint,
            json.dumps(data),
            headers,
            extra_options,
        )
        print("response: ")
        pprint(response.json())

    t_send_status = PythonOperator(
        task_id="send_status",
        python_callable=send_status_msg,
        provide_context=True,
        op_kwargs={
            "crypt_auth_tok": (
                encrypt_tok(airflow_conf.as_dict()["connections"]["APP_CLIENT_SECRET"]).decode()
            )
        },
    )

    t_create_tmpdir = CreateTmpDirOperator(task_id="create_temp_dir")
    t_cleanup_tmpdir = CleanupTmpDirOperator(task_id="cleanup_temp_dir")

    t_create_tmpdir >> t_find_uuid >> t_run_validation >> t_send_status >> t_cleanup_tmpdir
