import json
import unittest
from datetime import date
from functools import cached_property
from unittest.mock import MagicMock, patch

import requests
from status_change.callbacks.failure_callback import FailureCallback
from status_change.data_ingest_board_manager import DataIngestBoardManager
from status_change.slack.base import SlackMessage
from status_change.slack.reorganized import (
    SlackUploadReorganized,
    SlackUploadReorganizedNoDatasets,
    SlackUploadReorganizedPriority,
)
from status_change.slack_manager import SlackManager
from status_change.status_manager import (
    EntityUpdateException,
    EntityUpdater,
    StatusChanger,
    Statuses,
)
from status_change.status_utils import (
    get_data_ingest_board_query_url,
    get_entity_id,
    get_entity_ingest_url,
    get_env,
    get_globus_url,
    get_headers,
    get_project,
    is_internal_error,
)
from tests.fixtures import (
    dataset_context_mock_value,
    endpoints,
    good_upload_context,
    slack_upload_reorg_priority_str,
    slack_upload_reorg_str,
    upload_context_mock_value,
)
from utils import pythonop_set_dataset_state

from airflow.models.connection import Connection

conn_mock_hm = Connection(host=f"https://ingest.api.hubmapconsortium.org")


def get_mock_response(good: bool, response_data: bytes) -> requests.Response:
    mock_resp = requests.models.Response()
    mock_resp.url = "test_url"
    mock_resp.status_code = 200 if good else 400
    mock_resp.reason = "OK" if good else "Bad Request"
    mock_resp._content = response_data
    return mock_resp


class TestEntityUpdater(unittest.TestCase):
    validation_msg = "Test validation message"
    ingest_task = "Plugins passed: test_1, test_2"

    @cached_property
    @patch("status_change.status_manager.get_submission_context")
    def upload_entity_valid(self, context_mock):
        context_mock.return_value = good_upload_context
        return EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )

    def test_get_entity_type(self):
        assert self.upload_entity_valid.entity_type == "Upload"

    def test_fields_to_change(self):
        assert self.upload_entity_valid.fields_to_change == {
            "ingest_task": f"existing ingest_task text | {self.ingest_task}",
            "validation_message": self.validation_msg,
        }

    @patch("status_change.status_manager.EntityUpdater.validate_fields_to_change")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_update(self, conn_mock, hhr_mock, validate_mock):
        conn_mock.return_value = conn_mock_hm
        hhr_mock.assert_not_called()
        validate_mock.assert_not_called()
        self.upload_entity_valid.update()
        validate_mock.assert_called_once()
        hhr_mock.assert_called_once()

    @patch("status_change.status_manager.EntityUpdater.set_entity_api_data")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_manager.StatusChanger.update")
    @patch("status_change.status_manager.get_submission_context")
    def test_should_be_statuschanger(self, context_mock, sc_mock, hhr_mock, eu_mock):
        context_mock.return_value = good_upload_context
        hhr_mock.return_value.json.return_value = good_upload_context
        with_status = EntityUpdater(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"status": "valid", "validation_message": self.validation_msg},
            fields_to_append_to={"ingest_task": self.ingest_task},
        )
        sc_mock.assert_not_called()
        with_status.update()
        sc_mock.assert_called_once()
        eu_mock.assert_not_called()


class TestStatusChanger(unittest.TestCase):
    validation_msg = "Test validation message"

    @patch("status_change.status_manager.EntityUpdater.set_entity_api_data")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    @patch("status_change.status_manager.get_submission_context")
    def test_should_be_entityupdater(self, context_mock, sc_mock, hhr_mock, eu_mock):
        context_mock.return_value = good_upload_context
        hhr_mock.return_value.json.return_value = good_upload_context
        without_status = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            fields_to_overwrite={"validation_message": self.validation_msg},
        )
        assert without_status.status == None
        assert without_status.fields_to_change
        eu_mock.assert_not_called()
        without_status.update()
        eu_mock.assert_called_once()
        sc_mock.assert_not_called()

    @patch("status_change.status_manager.StatusChanger.call_message_managers")
    @patch("status_change.status_manager.EntityUpdater.set_entity_api_data")
    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_manager.StatusChanger.set_entity_api_data")
    @patch("status_change.status_manager.get_submission_context")
    def test_same_status(self, context_mock, sc_mock, hhr_mock, eu_mock, mm_mock):
        context_mock.return_value = good_upload_context
        hhr_mock.return_value.json.return_value = good_upload_context
        same_status = StatusChanger(
            "upload_valid_uuid",
            "upload_valid_token",
            status="new",
            fields_to_overwrite={"validation_message": self.validation_msg},
        )
        assert same_status.same_status == True
        assert same_status.status == Statuses.UPLOAD_NEW
        eu_mock.assert_not_called()
        same_status.update()
        eu_mock.assert_called_once()
        sc_mock.assert_not_called()
        mm_mock.assert_called_once()

    @cached_property  # could use lru_cache with project arg to make hubmap/sennet aware
    def upload_valid(self):
        with patch("status_change.status_utils.HttpHook.get_connection") as conn_mock:
            with patch("status_change.status_manager.get_submission_context") as mock_mthd:
                conn_mock.return_value = conn_mock_hm
                mock_mthd.return_value = good_upload_context
                return StatusChanger(
                    "upload_valid_uuid",
                    "upload_valid_token",
                    status="Valid",
                    extra_options={},
                )

    @cached_property
    def upload_invalid(self):
        with patch("status_change.status_utils.HttpHook.get_connection") as conn_mock:
            with patch("status_change.status_manager.get_submission_context") as mock_mthd:
                conn_mock.return_value = conn_mock_hm
                mock_mthd.return_value = good_upload_context
                return StatusChanger(
                    "upload_valid_uuid",
                    "upload_valid_token",
                    status="Invalid",
                    extra_options={},
                )

    def test_unrecognized_status(self):
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = good_upload_context
            with self.assertRaises(EntityUpdateException):
                # No "Published" status for uploads
                StatusChanger(
                    "invalid_status_uuid",
                    "invalid_status_token",
                    status="Published",
                    extra_options={},
                )

    def test_recognized_status(self):
        self.upload_valid.validate_fields_to_change()
        self.assertEqual(
            self.upload_valid.fields_to_change["status"],
            Statuses.get_status_str(self.upload_valid.status),
        )

    def test_extra_fields(self):
        with patch("status_change.status_manager.get_submission_context") as gsc_mock:
            gsc_mock.return_value = good_upload_context
            with_extra_field = StatusChanger(
                "extra_field_uuid",
                "extra_field_token",
                status=Statuses.UPLOAD_PROCESSING,
                fields_to_overwrite={"test_extra_field": True},
            )
            data = with_extra_field.fields_to_change
            self.assertIn("test_extra_field", data)
            self.assertEqual(data["test_extra_field"], True)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.put_request_to_entity_api")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_extra_field_good(self, conn_mock, entity_api_mock, context_mock):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run"):
            context_mock.return_value = {
                "status": "processing",
                "test_extra_field": False,
                "entity_type": "Upload",
            }
            with_extra_field = StatusChanger(
                "extra_options_uuid",
                "extra_options_token",
                status="valid",
                fields_to_overwrite={"test_extra_field": True},
                verbose=False,
            )
            with_extra_field.update()
            self.assertIn(
                {"test_extra_field": True, "status": "valid"}, entity_api_mock.call_args.args
            )

    @patch("status_change.status_utils.HttpHook.run")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_valid_status_in_request(self, conn_mock, hhr_mock):
        conn_mock.return_value = conn_mock_hm
        self.upload_valid.validate_fields_to_change()
        self.upload_valid.set_entity_api_data()
        self.assertIn('{"status": "valid"}', hhr_mock.call_args.args)

    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_invalid_status_in_request(self, conn_mock, ctx_mock):
        conn_mock.return_value = conn_mock_hm
        ctx_mock.return_value = {
            "status": "processing",
            "test_extra_field": True,
            "entity_type": "Upload",
        }
        with patch("status_change.status_utils.HttpHook.run"):
            sc = StatusChanger(
                "my_test_uuid",
                "my_test_token",
                status="Valid",
                extra_options={},
                fields_to_overwrite={"test_extra_field": False},
            )
            sc.validate_fields_to_change()
            sc.update()
            # patch the StatusChanger to avoid the tests in its constructor which
            # would normally detect an invalid update
            sc.fields_to_change["status"] = "published"
            with self.assertRaises((AssertionError, EntityUpdateException)):
                sc.update()

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_http_conn_id(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run") as httpr_mock:
            httpr_mock.return_value.json.return_value = good_upload_context
            with_http_conn_id = StatusChanger(
                "http_conn_uuid",
                "http_conn_token",
                status=Statuses.DATASET_NEW,
                http_conn_id="test_conn_id",
            )
            assert with_http_conn_id.http_conn_id == "test_conn_id"

    @patch("status_change.slack_manager.SlackManager.update")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_call_message_managers_valid_status(self, conn_mock, slack_mock):
        conn_mock.return_value = Connection(host=f"https://ingest.api.hubmapconsortium.org")
        self.assertFalse(slack_mock.called)
        self.assertEqual(self.upload_invalid.status, Statuses.UPLOAD_INVALID)
        with patch("status_change.slack_manager.post_to_slack_notify"):
            with patch("status_change.status_utils.HttpHook.run"):
                self.upload_invalid.update()
        self.assertTrue(slack_mock.called)

    @staticmethod
    def my_callable(**kwargs):
        return kwargs["uuid"]

    @patch("utils.StatusChanger")
    @patch("utils.get_auth_tok")
    def test_pythonop_set_dataset_state_valid(self, gat_mock, sc_mock):
        uuid = "test_uuid"
        token = "test_token"
        gat_mock.return_value = token
        message = "Test message"
        # Not passing a ds_state kwarg sets status to Processing
        dag_run_mock = MagicMock(conf={"dryrun": False})
        pythonop_set_dataset_state(
            crypt_auth_tok=token,
            dataset_uuid_callable=self.my_callable,
            uuid=uuid,
            message=message,
            dag_run=dag_run_mock,
        )
        sc_mock.assert_called_with(
            uuid,
            token,
            status="Processing",
            fields_to_overwrite={"pipeline_message": message},
            http_conn_id="entity_api_connection",
            reindex=True,
            run_id=None,
        )
        # Pass a valid ds_state and assert it was passed properly
        pythonop_set_dataset_state(
            crypt_auth_tok=token,
            dataset_uuid_callable=self.my_callable,
            uuid=uuid,
            message=message,
            ds_state="QA",
            dag_run=dag_run_mock,
        )
        sc_mock.assert_called_with(
            uuid,
            token,
            status="QA",
            fields_to_overwrite={"pipeline_message": message},
            http_conn_id="entity_api_connection",
            reindex=True,
            run_id=None,
        )

    @patch("utils.get_auth_tok")
    def test_pythonop_set_dataset_state_invalid(self, gat_mock):
        with patch("status_change.status_utils.HttpHook.run"):
            uuid = "test_uuid"
            token = "test_token"
            gat_mock.return_value = token
            message = "Test message"
            # Pass an invalid ds_state
            with self.assertRaises(Exception):
                pythonop_set_dataset_state(
                    crypt_auth_tok=token,
                    dataset_uuid_callable=self.my_callable,
                    uuid=uuid,
                    message=message,
                    ds_state="Unknown",
                )

    @patch("status_change.slack_manager.get_submission_context")
    @patch("status_change.slack.base.get_submission_context")
    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_manager.put_request_to_entity_api")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_triggered(
        self,
        conn_mock,
        entity_api_mock,
        sc_context_mock,
        slack_context_mock,
        slack_base_context_mock,
    ):
        conn_mock.return_value = Connection(host=f"https://ingest.api.hubmapconsortium.org")
        with patch("status_change.status_utils.HttpHook.run"):
            new_context = upload_context_mock_value
            sc_context_mock.return_value = new_context
            slack_context_mock.return_value = new_context
            slack_base_context_mock.return_value = new_context
            with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
                with patch("status_change.slack_manager.SlackManager.update") as update_mock:
                    StatusChanger(
                        "upload_valid_uuid",
                        "upload_valid_token",
                        status="Reorganized",
                        extra_options={},
                    ).update()
                    update_mock.assert_called_once()
                    entity_api_mock.assert_not_called()

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.slack_manager.SlackManager.update")
    @patch("status_change.status_manager.get_submission_context")
    def test_slack_not_triggered(self, context_mock, slack_mock, ingest_board_mock):
        with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
            context_mock.return_value = dataset_context_mock_value
            ingest_board_mock.return_value = {"entity_type": "dataset"}
            StatusChanger(
                "dataset_valid_uuid",
                "dataset_valid_token",
                status="Hold",
                extra_options={},
            ).update()
            slack_mock.assert_not_called()

    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.update")
    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.status_manager.get_submission_context")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_data_ingest_board_triggered(
        self, conn_mock, sc_context_mock, dib_context_mock, dib_update_mock
    ):
        conn_mock.return_value = conn_mock_hm
        with patch("status_change.status_utils.HttpHook.run"):
            sc_context_mock.return_value = upload_context_mock_value
            dib_context_mock.return_value = upload_context_mock_value
            with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
                with patch("status_change.slack_manager.get_submission_context"):
                    with patch("status_change.slack_manager.SlackManager.update"):
                        sc = StatusChanger(
                            "upload_valid_uuid",
                            "upload_valid_token",
                            status="Reorganized",
                            extra_options={},
                        )
                        sc.update()
                        dib_update_mock.assert_called_once()

    @patch("status_change.data_ingest_board_manager.get_submission_context")
    @patch("status_change.data_ingest_board_manager.DataIngestBoardManager.update")
    @patch("status_change.status_manager.get_submission_context")
    def test_data_ingest_board_not_triggered(self, context_mock, dib_mock, ingest_board_mock):
        with patch("status_change.status_manager.StatusChanger.set_entity_api_data"):
            context_mock.return_value = dataset_context_mock_value
            ingest_board_mock.return_value = {"entity_type": "dataset"}
            StatusChanger(
                "dataset_valid_uuid",
                "dataset_valid_token",
                status="Hold",
                extra_options={},
            ).update()
            dib_mock.assert_not_called()


class SlackTest(SlackMessage):
    name = "test_class"

    def format(self):
        return "I am formatted"


class TestSlack(unittest.TestCase):

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_message_formatting(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        status = Statuses.DATASET_DEPRECATED
        with patch.object(
            SlackManager,
            "status_to_class",
            {status: {"main_class": SlackTest, "subclasses": []}},
        ):
            mgr = self.slack_manager(status, {})
        assert mgr.message_class.format() == "I am formatted"

    @patch("status_change.status_utils.get_env")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_get_slack_channel(self, conn_mock, env_mock):
        conn_mock.return_value = conn_mock_hm
        env_mock.return_value = "prod"
        with patch.dict(
            "status_change.slack_manager.SlackManager.status_to_class",
            {"test_status": {"main_class": SlackTest, "subclasses": []}},
        ):
            with patch.dict(
                "status_change.status_utils.slack_channels", {"test_class": "test_channel"}
            ):
                mgr = self.slack_manager("test_status", {})
        assert mgr.message_class.channel == "test_channel"

    @patch("status_change.slack_manager.get_env")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_update_with_slack_channel(self, conn_mock, env_mock):
        """
        We expect to see prod environment use value in `slack_channels`,
        and non-prod envs to replace channel value with the
        value from `slack_channels_testing`.
        """
        conn_mock.return_value = conn_mock_hm
        with patch.dict(
            "status_change.status_utils.slack_channels",
            {"base": "base_channel", "test_class": "test_channel"},
        ):
            with patch.dict(
                "status_change.status_utils.slack_channels_testing",
                {"test_class": "base_channel"},
            ):
                with patch.dict(
                    "status_change.slack_manager.SlackManager.status_to_class",
                    {Statuses.UPLOAD_INVALID: {"main_class": SlackTest, "subclasses": []}},
                ):
                    for env_val, channel in {
                        "prod": "test_channel",
                        "dev": "base_channel",
                    }.items():
                        env_mock.return_value = env_val
                        mgr = self.slack_manager(Statuses.UPLOAD_INVALID, good_upload_context)
                        with patch(
                            "status_change.slack_manager.post_to_slack_notify"
                        ) as slack_mock:
                            mgr.update()
                            slack_mock.assert_called_once_with(
                                "test_token", "I am formatted", channel
                            )

    @patch("status_change.slack_manager.get_env")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_update_with_slack_channel_not_found(self, conn_mock, env_mock):
        """
        We expect to see "base" channel if channel not found for status
        in all environments.
        """
        conn_mock.return_value = conn_mock_hm
        with patch.dict(
            "status_change.status_utils.slack_channels",
            {"base": "base_channel"},
        ):
            with patch.dict(
                "status_change.status_utils.slack_channels_testing",
                {"base": "base_channel"},
            ):
                with patch.dict(
                    "status_change.slack_manager.SlackManager.status_to_class",
                    {Statuses.DATASET_HOLD: {"main_class": SlackTest, "subclasses": []}},
                ):
                    for env_val, channel in {
                        "prod": "base_channel",
                        "dev": "base_channel",
                    }.items():
                        env_mock.return_value = env_val
                        mgr = self.slack_manager(Statuses.DATASET_HOLD, good_upload_context)
                        with patch(
                            "status_change.slack_manager.post_to_slack_notify"
                        ) as slack_mock:
                            mgr.update()
                            slack_mock.assert_called_once_with(
                                "test_token", "I am formatted", channel
                            )

    @patch("status_change.slack.base.get_submission_context")
    @patch("status_change.slack_manager.get_submission_context")
    def slack_manager(self, status, context, context_mock, slack_mock):
        context_mock.return_value = context
        slack_mock.return_value = context
        with patch("status_change.slack_manager.SlackManager.update"):
            return SlackManager(status, "test_uuid", "test_token")

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_manager_main_class(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        mgr = self.slack_manager(
            Statuses.UPLOAD_REORGANIZED, good_upload_context | {"datasets": "present"}
        )
        assert type(mgr.message_class) is SlackUploadReorganized

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_manager_subclass(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        context_copy = good_upload_context.copy()
        context_copy.update(
            {"priority_project_list": ["PRIORITY"], "datasets": [{"dataset_type": "test_1"}]}
        )
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED, context_copy)
        assert type(mgr.message_class) is SlackUploadReorganizedPriority

    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_slack_manager_subclass_pass(self, conn_mock):
        conn_mock.return_value = conn_mock_hm
        mgr = self.slack_manager(Statuses.UPLOAD_REORGANIZED, good_upload_context)
        assert type(mgr.message_class) is SlackUploadReorganizedNoDatasets
        with self.assertRaises(NotImplementedError):
            mgr.message_class.format()
        assert mgr.update() == None

    @patch("status_change.slack_manager.SlackManager.update")
    def test_slack_manager_no_rule(self, update_mock):
        mgr = SlackManager(Statuses.DATASET_DEPRECATED, "test_uuid", "test_token")
        assert not mgr.is_valid_for_status
        update_mock.assert_not_called()

    @staticmethod
    def hhr_mock_side_effect(endpoint, headers, *args, **kwargs):
        context = {
            "uuid": "test_uuid",
            "datasets": [
                {
                    "uuid": "test_dataset_uuid",
                    "hubmap_id": "test_hubmap_id",
                    "dataset_type": "test_type",
                    "created_by_user_displayname": "test_user",
                    "created_by_user_email": "test_email",
                    "organ": "test_organ",
                    "priority_project_list": ["test_priority_project"],
                }
            ],
            "priority_project_list": ["test_priority_project"],
        }
        if "organs" in endpoint:
            return get_mock_response(True, json.dumps(context.get("datasets")).encode("utf-8"))
        if "file-system-abs-path" in endpoint:
            return get_mock_response(True, json.dumps({"path": "test_abs_path"}).encode("utf-8"))
        return get_mock_response(True, json.dumps(context).encode("utf-8"))

    @patch("status_change.status_utils.get_entity_ingest_url")
    @patch("status_change.status_utils.get_globus_url")
    @patch("status_change.status_utils.get_abs_path")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_reorganized_formatting(
        self, conn_mock, abs_path_mock, globus_url_mock, ingest_url_mock
    ):
        for klass, ret_val in {
            SlackUploadReorganized: slack_upload_reorg_str,
            SlackUploadReorganizedPriority: slack_upload_reorg_priority_str,
        }.items():
            with patch("utils.ENDPOINTS", endpoints["hubmap"]):
                conn_mock.return_value = Connection(
                    host=f"https://entity.api.hubmapconsortium.org"
                )
                with patch(
                    "status_change.status_utils.HttpHook.run",
                    side_effect=self.hhr_mock_side_effect,
                ):
                    abs_path_mock.return_value = "test_abs_path"
                    globus_url_mock.return_value = "test_globus_url"
                    ingest_url_mock.return_value = "test_ingest_ui_url"
                    mgr = klass("test_uuid", "test_token")
                    formatted = mgr.format()
                    lines = [line.strip() for line in formatted.split("\n") if line.strip()]
                    assert lines == ret_val


class TestFailureCallback(unittest.TestCase):
    @patch("utils.airflow_conf.as_dict")
    @patch("status_change.status_utils.get_submission_context")
    @patch("traceback.TracebackException.from_exception")
    @patch("status_change.callbacks.base.get_auth_tok")
    @patch("status_change.status_utils.HttpHook.get_connection")
    def test_failure_callback(self, conn_mock, gat_mock, tbfa_mock, gsc_mock, af_conf_mock):
        conn_mock.return_value = conn_mock_hm
        af_conf_mock.return_value = {"connections": {"WORKFLOW_SCRATCH": "/scratch/path"}}

        def _xcom_getter(key):
            return {"uuid": "abc123"}[key]

        class _exception_formatter:
            def __init__(self, excp_str):
                self.excp_str = excp_str

            def format(self):
                return f"This is the formatted version of {self.excp_str}"
        gsc_mock.return_value = self.context_mock_value
        gat_mock.return_value = "auth_token"
        tbfa_mock.from_exception = _exception_formatter
        dag_run_mock = MagicMock(
            conf={"dryrun": False},
            dag_id="test_dag_id",
            execution_date=date.fromisoformat("2025-06-05")
        )
        task_mock = MagicMock(task_id="mytaskid")
        task_instance_mock = MagicMock()
        task_instance_mock.xcom_pull.side_effect=_xcom_getter
        fcb = FailureCallback(__name__)
        tweaked_ctx = self.context_mock_value.copy()
        tweaked_ctx["task_instance"] = task_instance_mock
        tweaked_ctx["task"] = task_mock
        tweaked_ctx["crypt_auth_tok"] = "test_crypt_auth_tok"
        tweaked_ctx["dag_run"] = dag_run_mock
        tweaked_ctx["exception"] = "FakeTestException"
        with patch("status_change.status_manager.HttpHook.run") as hhr_mock:
            hhr_mock.return_value.json.return_value = self.good_context
            fcb(tweaked_ctx)
            args, kwargs = hhr_mock.call_args
            assert args[0] == "/entities/abc123"
            assert "mytaskid" in args[1]
            assert "test_dag_id" in args[1]
            assert "2025-06-05" in args[1]
            assert "mytaskid" in args[1]
            assert __name__ in args[1]
            assert "This is the formatted version of FakeTestException" in args[1]
            assert args[2]["authorization"] == "Bearer auth_token"

# if __name__ == "__main__":
#     suite = unittest.TestLoader().loadTestsFromTestCase(TestEntityUpdater)
#     suite.debug()
