"""Unit tests for HitlDecorator."""

# Import metaflow first to avoid circular imports when the extension module is
# loaded directly (the extension references metaflow.decorators.StepDecorator).
import metaflow  # noqa: F401

import os
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pytest

from metaflow_extensions.hitl.plugins.hitl_decorator import HitlDecorator, HitlException
from metaflow_extensions.hitl.plugins.approval_store import LocalApprovalStore


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _make_decorator(**kwargs):
    """Create a HitlDecorator with merged defaults."""
    defaults = dict(HitlDecorator.defaults)
    defaults.update(kwargs)
    deco = HitlDecorator.__new__(HitlDecorator)
    deco.attributes = defaults
    deco.logger = MagicMock()
    return deco


def _make_store(tmp_path):
    return LocalApprovalStore(base_dir=str(tmp_path))


def _make_flow(name="TestFlow", **attrs):
    flow = MagicMock()
    flow.__class__.__name__ = name
    for k, v in attrs.items():
        setattr(flow, k, v)
    return flow


# ------------------------------------------------------------------ #
# step_init validation                                                 #
# ------------------------------------------------------------------ #


def test_step_init_valid_on_timeout():
    deco = _make_decorator(on_timeout="approve")
    # Should not raise
    deco.step_init(
        MagicMock(), MagicMock(), "gate", [], MagicMock(), MagicMock(), MagicMock()
    )


def test_step_init_invalid_on_timeout():
    deco = _make_decorator(on_timeout="skip")
    with pytest.raises(HitlException):
        deco.step_init(
            MagicMock(), MagicMock(), "gate", [], MagicMock(), MagicMock(), MagicMock()
        )


def test_step_init_warns_no_notifier(recwarn):
    deco = _make_decorator(notifier=None)
    deco.step_init(
        MagicMock(), MagicMock(), "gate", [], MagicMock(), MagicMock(), MagicMock()
    )
    assert len(recwarn) == 1
    assert "notifier=None" in str(recwarn[0].message)


# ------------------------------------------------------------------ #
# _format_message                                                      #
# ------------------------------------------------------------------ #


def test_format_message_with_placeholder():
    deco = _make_decorator(message="Accuracy: {self.accuracy:.1%}")
    flow = _make_flow(accuracy=0.94)
    assert deco._format_message(flow) == "Accuracy: 94.0%"


def test_format_message_no_placeholder():
    deco = _make_decorator(message="Please review")
    flow = _make_flow()
    assert deco._format_message(flow) == "Please review"


def test_format_message_bad_placeholder():
    deco = _make_decorator(message="{self.missing_attr}")
    # spec=[] means no attributes exist, so {self.missing_attr} raises AttributeError
    flow = MagicMock(spec=[])
    # Should not raise; returns the raw template on error
    result = deco._format_message(flow)
    assert result == "{self.missing_attr}"


# ------------------------------------------------------------------ #
# _poll: approve path                                                  #
# ------------------------------------------------------------------ #


def test_poll_approve(tmp_path):
    store = _make_store(tmp_path)
    approval_id = store.create(
        flow_name="TestFlow",
        run_id="local-1",
        step_name="gate",
        message="",
        approvers=[],
        input_schema=None,
        timeout="1h",
    )
    store.approve(approval_id, {"target": "staging"})

    deco = _make_decorator()
    result = deco._poll(store, approval_id, "gate")
    assert result == {"target": "staging"}


# ------------------------------------------------------------------ #
# _poll: reject path                                                   #
# ------------------------------------------------------------------ #


def test_poll_reject(tmp_path):
    store = _make_store(tmp_path)
    approval_id = store.create(
        flow_name="TestFlow",
        run_id="local-1",
        step_name="gate",
        message="",
        approvers=[],
        input_schema=None,
        timeout="1h",
    )
    store.reject(approval_id, reason="not ready")

    deco = _make_decorator()
    with pytest.raises(HitlException, match="not ready"):
        deco._poll(store, approval_id, "gate")


# ------------------------------------------------------------------ #
# _poll: timeout → reject                                              #
# ------------------------------------------------------------------ #


def test_poll_timeout_reject(tmp_path):
    store = _make_store(tmp_path)
    approval_id = store.create(
        flow_name="TestFlow",
        run_id="local-1",
        step_name="gate",
        message="",
        approvers=[],
        input_schema=None,
        timeout="1h",
    )
    # Manually backdate expires_at so it's already expired
    from datetime import timezone
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    store._update(approval_id, {"expires_at": past})

    deco = _make_decorator(on_timeout="reject", poll_interval=0)
    with pytest.raises(HitlException, match="timed out"):
        deco._poll(store, approval_id, "gate")


# ------------------------------------------------------------------ #
# _poll: timeout → approve                                             #
# ------------------------------------------------------------------ #


def test_poll_timeout_approve(tmp_path):
    store = _make_store(tmp_path)
    approval_id = store.create(
        flow_name="TestFlow",
        run_id="local-1",
        step_name="gate",
        message="",
        approvers=[],
        input_schema=None,
        timeout="1h",
    )
    past = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    store._update(approval_id, {"expires_at": past})

    deco = _make_decorator(on_timeout="approve", poll_interval=0)
    result = deco._poll(store, approval_id, "gate")
    assert result == {}
    assert store.get(approval_id)["status"] == "approved"


# ------------------------------------------------------------------ #
# task_post_step sets hitl_input if missing                            #
# ------------------------------------------------------------------ #


def test_task_post_step_sets_empty_hitl_input():
    deco = _make_decorator()
    flow = MagicMock(spec=[])  # no attributes
    deco.task_post_step("gate", flow, MagicMock(), 0, 0)
    assert flow.hitl_input == {}


def test_task_post_step_preserves_existing_hitl_input():
    deco = _make_decorator()
    flow = MagicMock()
    flow.hitl_input = {"key": "value"}
    deco.task_post_step("gate", flow, MagicMock(), 0, 0)
    assert flow.hitl_input == {"key": "value"}


# ------------------------------------------------------------------ #
# _build_notifier                                                      #
# ------------------------------------------------------------------ #


def test_build_notifier_none():
    deco = _make_decorator(notifier=None)
    assert deco._build_notifier() is None


def test_build_notifier_unknown():
    deco = _make_decorator(notifier="carrier_pigeon")
    with pytest.raises(HitlException):
        deco._build_notifier()


def test_build_notifier_instance():
    notifier = MagicMock()
    deco = _make_decorator(notifier=notifier)
    assert deco._build_notifier() is notifier


def test_build_notifier_apprise_str():
    from metaflow_extensions.hitl.plugins.notifiers.apprise_notifier import AppriseNotifier

    deco = _make_decorator(notifier="apprise")
    result = deco._build_notifier()
    assert isinstance(result, AppriseNotifier)


def test_build_notifier_apprise_list():
    from metaflow_extensions.hitl.plugins.notifiers.apprise_notifier import AppriseNotifier

    urls = ["slack://token/channel", "mailto://user:pass@example.com"]
    deco = _make_decorator(notifier=urls)
    result = deco._build_notifier()
    assert isinstance(result, AppriseNotifier)
    assert result._urls == urls


def test_apprise_notifier_send_calls_notify():
    from metaflow_extensions.hitl.plugins.notifiers.apprise_notifier import AppriseNotifier

    notifier = AppriseNotifier(urls=["json://localhost"])

    with patch("apprise.Apprise") as MockApprise:
        instance = MockApprise.return_value
        notifier.send(
            approval_id="abc-123",
            flow_name="TestFlow",
            run_id="run-1",
            step_name="gate",
            message="Please review",
            argo_workflow_name="argo-test",
            argo_resume_cmd="argo resume argo-test",
            expires_at="2099-01-01T00:00:00+00:00",
        )
        instance.notify.assert_called_once()
        call_kwargs = instance.notify.call_args
        assert "HITL Approval Required" in call_kwargs.kwargs["title"]
        assert "abc-123" in call_kwargs.kwargs["body"]
        assert "Please review" in call_kwargs.kwargs["body"]


def test_apprise_notifier_reads_env_urls(monkeypatch):
    from metaflow_extensions.hitl.plugins.notifiers.apprise_notifier import AppriseNotifier

    monkeypatch.setenv(
        "METAFLOW_HITL_APPRISE_URLS",
        "slack://token/channel,mailto://user:pass@example.com",
    )
    notifier = AppriseNotifier()
    assert notifier._urls == ["slack://token/channel", "mailto://user:pass@example.com"]


# ------------------------------------------------------------------ #
# task_pre_step on Argo (post-suspend): loads hitl_input              #
# ------------------------------------------------------------------ #


def test_task_pre_step_argo_loads_hitl_input(tmp_path, monkeypatch):
    monkeypatch.setenv("ARGO_WORKFLOW_NAME", "argo-test-abc")
    monkeypatch.setenv("METAFLOW_FLOW_NAME", "TestFlow")
    monkeypatch.setenv("METAFLOW_DEFAULT_DATASTORE", "local")

    store = _make_store(tmp_path)
    approval_id = store.create(
        flow_name="TestFlow",
        run_id="argo-test-abc",
        step_name="gate",
        message="",
        approvers=[],
        input_schema=None,
        timeout="1h",
    )
    store.approve(approval_id, {"deploy_target": "prod"})

    deco = _make_decorator()
    flow = _make_flow("TestFlow")

    with patch(
        "metaflow_extensions.hitl.plugins.approval_store.ApprovalStore.from_environment",
        return_value=store,
    ):
        deco.task_pre_step(
            step_name="gate",
            task_datastore=MagicMock(),
            metadata=MagicMock(),
            run_id="argo-test-abc",
            graph=MagicMock(),
            flow=flow,
            ubf_context=None,
            inputs=None,
        )

    assert flow.hitl_input == {"deploy_target": "prod"}
