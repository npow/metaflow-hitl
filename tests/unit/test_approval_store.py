"""Unit tests for ApprovalStore backends."""

import json
import os
import tempfile
from datetime import datetime, timezone

import pytest

from metaflow_extensions.hitl.plugins.approval_store import LocalApprovalStore


@pytest.fixture
def store(tmp_path):
    return LocalApprovalStore(base_dir=str(tmp_path))


def _make_approval(store, **kwargs):
    defaults = dict(
        flow_name="TestFlow",
        run_id="local-123",
        step_name="gate",
        message="Please review",
        approvers=["alice@example.com"],
        input_schema=None,
        timeout="24h",
        argo_workflow_name=None,
    )
    defaults.update(kwargs)
    return store.create(**defaults)


# ------------------------------------------------------------------ #
# create / get                                                         #
# ------------------------------------------------------------------ #


def test_create_returns_id(store):
    approval_id = _make_approval(store)
    assert isinstance(approval_id, str)
    assert len(approval_id) > 0


def test_get_returns_record(store):
    approval_id = _make_approval(store)
    record = store.get(approval_id)
    assert record["approval_id"] == approval_id
    assert record["status"] == "pending"
    assert record["flow_name"] == "TestFlow"
    assert record["step_name"] == "gate"


def test_get_missing_raises(store):
    with pytest.raises(KeyError):
        store.get("nonexistent-id")


def test_record_schema(store):
    approval_id = _make_approval(
        store,
        argo_workflow_name="argo-test-abc",
        input_schema={"deploy_target": "str"},
    )
    record = store.get(approval_id)
    required_fields = [
        "approval_id",
        "flow_name",
        "run_id",
        "step_name",
        "status",
        "message",
        "approvers",
        "input_schema",
        "hitl_input",
        "argo_workflow_name",
        "argo_namespace",
        "created_at",
        "expires_at",
        "rejection_reason",
    ]
    for field in required_fields:
        assert field in record, "Missing field: %s" % field

    assert record["argo_workflow_name"] == "argo-test-abc"
    assert record["input_schema"] == {"deploy_target": "str"}
    assert record["hitl_input"] is None


# ------------------------------------------------------------------ #
# approve                                                              #
# ------------------------------------------------------------------ #


def test_approve_sets_status(store):
    approval_id = _make_approval(store)
    store.approve(approval_id, {"deploy_target": "staging"})
    record = store.get(approval_id)
    assert record["status"] == "approved"
    assert record["hitl_input"] == {"deploy_target": "staging"}


def test_approve_empty_input(store):
    approval_id = _make_approval(store)
    store.approve(approval_id)
    record = store.get(approval_id)
    assert record["status"] == "approved"
    assert record["hitl_input"] == {}


# ------------------------------------------------------------------ #
# reject                                                               #
# ------------------------------------------------------------------ #


def test_reject_sets_status(store):
    approval_id = _make_approval(store)
    store.reject(approval_id, reason="accuracy too low")
    record = store.get(approval_id)
    assert record["status"] == "rejected"
    assert record["rejection_reason"] == "accuracy too low"


def test_reject_no_reason(store):
    approval_id = _make_approval(store)
    store.reject(approval_id)
    record = store.get(approval_id)
    assert record["status"] == "rejected"
    assert record["rejection_reason"] == ""


# ------------------------------------------------------------------ #
# list_pending                                                         #
# ------------------------------------------------------------------ #


def test_list_pending_returns_pending_only(store):
    id1 = _make_approval(store, step_name="gate1")
    id2 = _make_approval(store, step_name="gate2")
    store.approve(id2)
    pending = store.list_pending()
    pending_ids = [r["approval_id"] for r in pending]
    assert id1 in pending_ids
    assert id2 not in pending_ids


def test_list_pending_filter_by_flow(store):
    _make_approval(store, flow_name="FlowA")
    _make_approval(store, flow_name="FlowB")
    pending_a = store.list_pending(flow_name="FlowA")
    assert all(r["flow_name"] == "FlowA" for r in pending_a)
    assert len(pending_a) == 1


def test_list_pending_empty(store):
    assert store.list_pending() == []


# ------------------------------------------------------------------ #
# duration parsing                                                     #
# ------------------------------------------------------------------ #


def test_duration_minutes(store):
    approval_id = _make_approval(store, timeout="30m")
    record = store.get(approval_id)
    created = datetime.fromisoformat(record["created_at"])
    expires = datetime.fromisoformat(record["expires_at"])
    diff = (expires - created).total_seconds()
    assert abs(diff - 1800) < 5


def test_duration_hours(store):
    approval_id = _make_approval(store, timeout="4h")
    record = store.get(approval_id)
    created = datetime.fromisoformat(record["created_at"])
    expires = datetime.fromisoformat(record["expires_at"])
    diff = (expires - created).total_seconds()
    assert abs(diff - 4 * 3600) < 5


def test_duration_days(store):
    approval_id = _make_approval(store, timeout="7d")
    record = store.get(approval_id)
    created = datetime.fromisoformat(record["created_at"])
    expires = datetime.fromisoformat(record["expires_at"])
    diff = (expires - created).total_seconds()
    assert abs(diff - 7 * 86400) < 5


def test_invalid_duration(store):
    from metaflow_extensions.hitl.plugins.approval_store import _parse_duration

    with pytest.raises(ValueError):
        _parse_duration("5x")
