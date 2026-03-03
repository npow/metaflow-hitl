"""
Integration tests for the metaflow-hitl Argo Workflows end-to-end flow.

Requires a running devstack with:
    - argo-workflows
    - minio (S3-compatible)
    - metadata-service

Start devstack:
    cd /root/code/metaflow/devtools
    SERVICES_OVERRIDE=argo-workflows,minio,metadata-service make up
    make shell   # in another terminal

Run tests:
    cd /root/code/metaflow-hitl
    pytest tests/integration/ -v
"""

import os
import time

import pytest
import requests

# Skip all integration tests if ARGO_SERVER is not set.
pytestmark = pytest.mark.skipif(
    not os.environ.get("ARGO_SERVER"),
    reason="ARGO_SERVER not set — skipping integration tests (requires devstack)",
)

ARGO_SERVER = os.environ.get("ARGO_SERVER", "http://localhost:2746")
ARGO_NAMESPACE = os.environ.get("ARGO_NAMESPACE", "argo")


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #


def _wait_for_status(workflow_name, target_status, timeout=300, poll=5):
    """Poll Argo API until workflow reaches target_status or times out."""
    url = "%s/api/v1/workflows/%s/%s" % (ARGO_SERVER, ARGO_NAMESPACE, workflow_name)
    deadline = time.time() + timeout
    while time.time() < deadline:
        resp = requests.get(url)
        resp.raise_for_status()
        status = resp.json().get("status", {}).get("phase")
        if status == target_status:
            return True
        if status in ("Failed", "Error"):
            return False
        time.sleep(poll)
    raise TimeoutError(
        "Workflow %s did not reach '%s' within %ds" % (workflow_name, target_status, timeout)
    )


def _resume_workflow(workflow_name):
    url = "%s/api/v1/workflows/%s/%s/resume" % (
        ARGO_SERVER,
        ARGO_NAMESPACE,
        workflow_name,
    )
    resp = requests.put(url, json={})
    resp.raise_for_status()


def _get_latest_workflow(flow_name):
    """Return the name of the most recently created workflow for a flow."""
    url = "%s/api/v1/workflows/%s" % (ARGO_SERVER, ARGO_NAMESPACE)
    resp = requests.get(url, params={"listOptions.labelSelector": "app.kubernetes.io/name=metaflow-run"})
    resp.raise_for_status()
    workflows = resp.json().get("items") or []
    # Filter by flow name annotation / label
    matching = [
        w for w in workflows
        if w.get("metadata", {}).get("annotations", {}).get("metaflow/flow_name") == flow_name
        or flow_name.lower() in w.get("metadata", {}).get("name", "")
    ]
    if not matching:
        raise RuntimeError("No workflows found for flow '%s'" % flow_name)
    return sorted(matching, key=lambda w: w["metadata"]["creationTimestamp"])[-1]["metadata"]["name"]


# ------------------------------------------------------------------ #
# Sample flow definition                                               #
# ------------------------------------------------------------------ #

HITL_FLOW_CODE = '''
from metaflow import FlowSpec, step
from metaflow_extensions.hitl.plugins.hitl_decorator import HitlDecorator as hitl

class HitlTestFlow(FlowSpec):
    @step
    def start(self):
        self.value = 42
        self.next(self.gate)

    @hitl(
        approvers=[],
        message="Review value={self.value}",
        timeout="1h",
        on_timeout="reject",
        notifier=None,
        input_schema={"deploy_target": "str"},
    )
    @step
    def gate(self):
        print("hitl_input:", self.hitl_input)
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    HitlTestFlow()
'''


# ------------------------------------------------------------------ #
# Tests                                                                #
# ------------------------------------------------------------------ #


@pytest.fixture(scope="module")
def flow_file(tmp_path_factory):
    """Write the sample flow to a temp file."""
    p = tmp_path_factory.mktemp("flows") / "hitl_test_flow.py"
    p.write_text(HITL_FLOW_CODE)
    return str(p)


def test_hitl_argo_approve(flow_file):
    """Full approve path: suspend → approve → complete."""
    import subprocess

    # Deploy + trigger
    subprocess.check_call(
        ["python", flow_file, "argo-workflows", "create"], timeout=60
    )
    subprocess.check_call(
        ["python", flow_file, "argo-workflows", "trigger"], timeout=60
    )

    workflow_name = _get_latest_workflow("HitlTestFlow")

    # Wait for Suspended
    assert _wait_for_status(workflow_name, "Running", timeout=120)
    # Wait until a node is in Suspended phase
    time.sleep(10)

    # Read approval record
    from metaflow_extensions.hitl.plugins.approval_store import ApprovalStore

    store = ApprovalStore.from_environment()
    pending = store.list_pending(flow_name="HitlTestFlow")
    assert len(pending) >= 1, "Expected at least one pending approval"
    approval_id = pending[0]["approval_id"]

    # Approve
    store.approve(approval_id, {"deploy_target": "staging"})

    # Resume Argo workflow
    _resume_workflow(workflow_name)

    # Wait for completion
    assert _wait_for_status(workflow_name, "Succeeded", timeout=180)

    # Verify hitl_input artifact
    from metaflow import Flow

    run = Flow("HitlTestFlow").latest_run
    assert run["gate"].task.data.hitl_input == {"deploy_target": "staging"}


def test_hitl_argo_reject(flow_file):
    """Reject path: suspend → reject → workflow fails."""
    import subprocess

    subprocess.check_call(
        ["python", flow_file, "argo-workflows", "trigger"], timeout=60
    )

    workflow_name = _get_latest_workflow("HitlTestFlow")

    # Wait until Running/Suspended
    assert _wait_for_status(workflow_name, "Running", timeout=120)
    time.sleep(10)

    from metaflow_extensions.hitl.plugins.approval_store import ApprovalStore

    store = ApprovalStore.from_environment()
    pending = store.list_pending(flow_name="HitlTestFlow")
    assert len(pending) >= 1
    approval_id = pending[0]["approval_id"]

    # Reject
    store.reject(approval_id, reason="accuracy too low")

    # Resume Argo workflow (so the gate step runs and raises HitlException)
    _resume_workflow(workflow_name)

    # Wait — workflow should fail
    result = _wait_for_status(workflow_name, "Failed", timeout=180)
    assert result, "Expected workflow to fail after rejection"
