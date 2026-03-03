"""
CLI extension for managing HITL approvals.

Usage (as a Metaflow CLI extension):
    python flow.py hitl list
    python flow.py hitl approve <approval_id>
    python flow.py hitl approve <approval_id> --input '{"deploy_target": "staging"}'
    python flow.py hitl reject <approval_id> --reason "accuracy too low"

Usage (standalone):
    python -m metaflow_extensions.hitl.plugins.hitl_cli list
"""

import json
import sys

import click


@click.group()
def cli():
    """Human-in-the-loop approval management."""
    pass


@cli.command("list")
@click.option("--flow-name", default=None, help="Filter by flow name")
def list_pending(flow_name):
    """List pending approvals."""
    from .approval_store import ApprovalStore

    store = ApprovalStore.from_environment()
    records = store.list_pending(flow_name=flow_name)

    if not records:
        click.echo("No pending approvals.")
        return

    for r in records:
        click.echo(
            "[{status}] {approval_id}  flow={flow_name}  run={run_id}  "
            "step={step_name}  expires={expires_at}".format(**r)
        )


@cli.command("approve")
@click.argument("approval_id")
@click.option(
    "--input",
    "hitl_input_str",
    default=None,
    help="JSON string of structured input, e.g. '{\"deploy_target\": \"staging\"}'",
)
def approve(approval_id, hitl_input_str):
    """Approve a pending approval gate."""
    from .approval_store import ApprovalStore

    store = ApprovalStore.from_environment()

    hitl_input = {}
    if hitl_input_str:
        try:
            hitl_input = json.loads(hitl_input_str)
        except json.JSONDecodeError as exc:
            click.echo("Invalid JSON for --input: %s" % exc, err=True)
            sys.exit(1)

    store.approve(approval_id, hitl_input)
    click.echo("Approved: %s" % approval_id)


@cli.command("reject")
@click.argument("approval_id")
@click.option("--reason", default="", help="Rejection reason")
def reject(approval_id, reason):
    """Reject a pending approval gate."""
    from .approval_store import ApprovalStore

    store = ApprovalStore.from_environment()
    store.reject(approval_id, reason=reason)
    click.echo("Rejected: %s" % approval_id)


if __name__ == "__main__":
    cli()
