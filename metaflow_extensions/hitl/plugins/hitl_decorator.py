import os
import time
import warnings
from datetime import datetime, timezone

from metaflow.decorators import StepDecorator
from metaflow.exception import MetaflowException


class HitlException(MetaflowException):
    headline = "HITL approval error"


class HitlDecorator(StepDecorator):
    """
    Adds a human-in-the-loop approval gate to a step.

    On Argo Workflows the gate is implemented as a native Argo ``suspend``
    template, meaning zero compute cost during the approval window.  On local
    runs (or any non-Argo orchestrator) the decorator falls back to polling.

    Parameters
    ----------
    approvers : list[str], optional
        Email addresses / usernames to notify.
    message : str, optional
        Notification message.  Use ``{self.attr}`` placeholders — they are
        formatted at runtime against the flow object.
    timeout : str, default ``"24h"``
        Approval window.  Accepted units: ``m`` (minutes), ``h`` (hours),
        ``d`` (days).  Example: ``"30m"``, ``"4h"``, ``"7d"``.
    on_timeout : str, default ``"reject"``
        What to do when the approval window expires.
        ``"reject"`` — raise an exception, ``"approve"`` — auto-approve,
        ``"notify"`` — send another notification and keep waiting.
    notifier : str or None, optional
        ``None``, ``"slack"``, or ``"smtp"``.
    input_schema : dict or None, optional
        Mapping of ``{field_name: type_str}`` for structured approval input,
        e.g. ``{"deploy_target": "str", "notes": "str"}``.
    poll_interval : int, default 30
        Seconds between store polls (non-Argo runs only).
    """

    name = "hitl"
    defaults = {
        "approvers": [],
        "message": "",
        "timeout": "24h",
        "on_timeout": "reject",
        "notifier": None,
        "input_schema": None,
        "poll_interval": 30,
    }

    # ------------------------------------------------------------------ #
    # Lifecycle hooks                                                       #
    # ------------------------------------------------------------------ #

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        self.logger = logger
        on_timeout = self.attributes["on_timeout"]
        if on_timeout not in ("reject", "approve", "notify"):
            raise HitlException(
                "@hitl on step '%s': on_timeout must be one of "
                "'reject', 'approve', or 'notify', got '%s'" % (step, on_timeout)
            )
        if self.attributes["notifier"] is None:
            warnings.warn(
                "@hitl on step '%s': notifier=None — no notification will be sent. "
                "Set notifier='slack' or notifier='smtp' to enable notifications."
                % step,
                stacklevel=2,
            )

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        metadata,
        run_id,
        graph,
        flow,
        ubf_context,
        inputs,
    ):
        """Gate logic.

        On Argo: called *after* the native suspend has been resumed.  The
        decorator simply reads the approval record from the store and sets
        ``flow.hitl_input``.

        On non-Argo orchestrators: creates an approval record, sends the
        notification, and polls until approved / rejected / expired.
        """
        from .approval_store import ApprovalStore

        store = ApprovalStore.from_environment()
        flow_name = flow.__class__.__name__

        if self._is_argo():
            # Execution has already been suspended and resumed — just load the
            # approval record and expose hitl_input.
            approval_id = self._get_argo_approval_id(run_id, step_name)
            if approval_id:
                try:
                    record = store.get(approval_id)
                    flow.hitl_input = record.get("hitl_input") or {}
                    if record.get("status") == "rejected":
                        raise HitlException(
                            "Step '%s' was rejected: %s"
                            % (step_name, record.get("rejection_reason", ""))
                        )
                except KeyError:
                    # Approval record missing — proceed without hitl_input
                    flow.hitl_input = {}
            else:
                flow.hitl_input = {}
        else:
            # Local / non-Argo run: create record, notify, poll.
            argo_workflow_name = os.environ.get("ARGO_WORKFLOW_NAME")
            argo_namespace = os.environ.get(
                "ARGO_WORKFLOW_NAMESPACE", "default"
            )

            message = self._format_message(flow)

            approval_id = store.create(
                flow_name=flow_name,
                run_id=run_id,
                step_name=step_name,
                message=message,
                approvers=self.attributes["approvers"],
                input_schema=self.attributes["input_schema"],
                timeout=self.attributes["timeout"],
                argo_workflow_name=argo_workflow_name,
                argo_namespace=argo_namespace,
            )

            notifier = self._build_notifier()
            if notifier:
                record = store.get(approval_id)
                argo_resume_cmd = (
                    "argo -n %s resume %s" % (argo_namespace, argo_workflow_name)
                    if argo_workflow_name
                    else "n/a (not running on Argo)"
                )
                notifier.send(
                    approval_id=approval_id,
                    flow_name=flow_name,
                    run_id=run_id,
                    step_name=step_name,
                    message=message,
                    argo_workflow_name=argo_workflow_name,
                    argo_resume_cmd=argo_resume_cmd,
                    expires_at=record["expires_at"],
                )

            flow.hitl_input = self._poll(store, approval_id, step_name)

    def task_post_step(
        self, step_name, flow, graph, retry_count, max_user_code_retries
    ):
        # Ensure hitl_input is always persisted even when no schema is defined.
        if not hasattr(flow, "hitl_input"):
            flow.hitl_input = {}

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _is_argo(self):
        return bool(
            os.environ.get("ARGO_WORKFLOWS_WORKFLOW_NAME")
            or os.environ.get("METAFLOW_ARGO_WORKFLOWS_WORKFLOW_NAME")
            or os.environ.get("ARGO_WORKFLOW_NAME")
        )

    def _get_argo_approval_id(self, run_id, step_name):
        """Try to retrieve the approval_id written by notify_entrypoint."""
        from .approval_store import ApprovalStore

        store = ApprovalStore.from_environment()
        # The run_id on Argo looks like "argo-<workflow-name>"
        flow_name = os.environ.get("METAFLOW_FLOW_NAME", "")
        try:
            records = store.list_all(flow_name)
            for r in records:
                if r.get("run_id") == run_id and r.get("step_name") == step_name:
                    return r["approval_id"]
        except Exception:
            pass
        return None

    def _poll(self, store, approval_id, step_name):
        """Block until the approval is resolved or expires. Returns hitl_input."""
        poll_interval = self.attributes["poll_interval"]
        on_timeout = self.attributes["on_timeout"]

        while True:
            record = store.get(approval_id)
            status = record["status"]

            if status == "approved":
                return record.get("hitl_input") or {}

            if status == "rejected":
                raise HitlException(
                    "Step '%s' was rejected: %s"
                    % (step_name, record.get("rejection_reason", ""))
                )

            # Check expiry
            expires_at = datetime.fromisoformat(record["expires_at"])
            if expires_at.tzinfo is None:
                expires_at = expires_at.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) >= expires_at:
                if on_timeout == "approve":
                    store.approve(approval_id, {})
                    return {}
                elif on_timeout == "notify":
                    notifier = self._build_notifier()
                    if notifier:
                        argo_workflow_name = record.get("argo_workflow_name")
                        argo_namespace = record.get("argo_namespace", "default")
                        argo_resume_cmd = (
                            "argo -n %s resume %s"
                            % (argo_namespace, argo_workflow_name)
                            if argo_workflow_name
                            else "n/a"
                        )
                        notifier.send(
                            approval_id=approval_id,
                            flow_name=record["flow_name"],
                            run_id=record["run_id"],
                            step_name=step_name,
                            message="[REMINDER] " + (record.get("message") or ""),
                            argo_workflow_name=argo_workflow_name,
                            argo_resume_cmd=argo_resume_cmd,
                            expires_at=record["expires_at"],
                        )
                    # Keep polling for another timeout period
                    from .approval_store import _parse_duration
                    new_expires = datetime.now(timezone.utc) + _parse_duration(
                        self.attributes["timeout"]
                    )
                    store._update(
                        approval_id,
                        {"expires_at": new_expires.isoformat()},
                    )
                else:  # "reject"
                    store.reject(approval_id, "Approval timed out")
                    raise HitlException(
                        "Step '%s' approval timed out" % step_name
                    )

            time.sleep(poll_interval)

    def _format_message(self, flow):
        msg = self.attributes.get("message") or ""
        try:
            return msg.format(self=flow)
        except Exception:
            return msg

    def _build_notifier(self):
        notifier = self.attributes.get("notifier")
        if notifier is None:
            return None
        if isinstance(notifier, list):
            # Treat a list as Apprise URLs
            from .notifiers.apprise_notifier import AppriseNotifier
            return AppriseNotifier(urls=notifier)
        if isinstance(notifier, str):
            if notifier == "slack":
                from .notifiers.slack import SlackNotifier
                return SlackNotifier()
            elif notifier == "smtp":
                from .notifiers.smtp import SmtpNotifier
                return SmtpNotifier()
            elif notifier == "apprise":
                from .notifiers.apprise_notifier import AppriseNotifier
                return AppriseNotifier()  # reads METAFLOW_HITL_APPRISE_URLS
            else:
                raise HitlException("Unknown notifier '%s'" % notifier)
        # Already a Notifier instance
        return notifier
