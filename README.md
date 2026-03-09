# metaflow-hitl

[![CI](https://github.com/npow/metaflow-hitl/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/metaflow-hitl/actions/workflows/ci.yml)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![WIP](https://img.shields.io/badge/status-WIP-orange.svg)](#) [![Docs](https://img.shields.io/badge/docs-mintlify-18a34a?style=flat-square)](https://mintlify.com/npow/metaflow-hitl)

Add human approval gates to your Metaflow pipelines without burning GPU money while you wait.

## The problem

When an ML pipeline reaches a critical decision — deploy a model, trigger a retrain, promote to production — you need a human to review and approve before it proceeds. Today your options are bad: block an expensive compute pod for however long it takes a human to respond, or build a bespoke polling/callback system outside the flow. Neither captures the approval decision as a first-class artifact with lineage.

## Quick start

```bash
pip install metaflow-hitl
```

```python
from metaflow import FlowSpec, step
from metaflow_extensions.hitl.plugins.hitl_decorator import hitl

class TrainAndDeploy(FlowSpec):
    @step
    def train(self):
        self.accuracy = 0.94
        self.next(self.gate)

    @hitl(
        message="Accuracy: {self.accuracy:.1%} — approve to deploy?",
        timeout="24h",
        on_timeout="reject",
        notifier="apprise",
    )
    @step
    def gate(self):
        # self.hitl_input is set from the approval response
        print("Approved for:", self.hitl_input.get("deploy_target"))
        self.next(self.end)

    @step
    def end(self):
        pass

if __name__ == "__main__":
    TrainAndDeploy()
```

## Install

```bash
# From PyPI (once published)
pip install metaflow-hitl

# From source
git clone https://github.com/npow/metaflow-hitl
pip install -e .
```

Both `metaflow` and `metaflow-hitl` must be installed in the same environment.

## Usage

### Approve or reject from the CLI

```bash
# List pending approvals
python flow.py hitl list

# Approve with structured input
python flow.py hitl approve <approval_id> --input '{"deploy_target": "staging"}'

# Reject with a reason
python flow.py hitl reject <approval_id> --reason "accuracy too low"
```

### Structured approval input

```python
@hitl(
    message="Deploy {self.model_name} v{self.version}?",
    input_schema={"deploy_target": "str", "notes": "str"},
    timeout="48h",
    on_timeout="reject",
)
@step
def gate(self):
    target = self.hitl_input["deploy_target"]
    notes  = self.hitl_input.get("notes", "")
```

### Timeout behaviour

```python
# Auto-approve after 24h if no response (e.g. for low-stakes gates)
@hitl(timeout="24h", on_timeout="approve")
@step
def soft_gate(self): ...

# Fail the run if nobody responds in time (default, use for critical paths)
@hitl(timeout="4h", on_timeout="reject")
@step
def hard_gate(self): ...
```

## How it works

On **Argo Workflows**, `@hitl` injects two synthetic DAG steps before the decorated step:

1. `hitl-notify-{step}` — a lightweight container that records the approval request in S3 and sends a notification (Slack, email, or any [Apprise](https://github.com/caronc/apprise)-compatible URL). Runs for seconds then exits.
2. `hitl-suspend-{step}` — a native Argo [`suspend` template](https://argo-workflows.readthedocs.io/en/latest/walk-through/suspending/). Zero running pods until a human calls `argo resume` or the Argo API.

After resume, the original step runs normally and finds `self.hitl_input` pre-loaded from the approval record.

On **local runs**, the decorator falls back to polling the local approval store, so the same flow works for development without Argo.

## Configuration

| Environment variable | Purpose |
|---|---|
| `METAFLOW_HITL_APPRISE_URLS` | Comma-separated [Apprise URLs](https://github.com/caronc/apprise/wiki) for notifications |
| `METAFLOW_HITL_ARGO_UI_URL` | Base URL of your Argo Workflows UI (included in notifications) |
| `METAFLOW_HITL_ARGO_NAMESPACE` | Kubernetes namespace for Argo (default: `argo`) |

## Development

```bash
git clone https://github.com/npow/metaflow-hitl
pip install -e metaflow          # install metaflow from source
pip install -e metaflow-hitl     # install this package

# Run unit tests (no Argo required)
pytest tests/unit/ -v

# Run integration tests (requires Argo + MinIO devstack)
pytest tests/integration/ -v
```

## License

[Apache 2.0](LICENSE)
