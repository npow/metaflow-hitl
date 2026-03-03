import json
import os

import requests

from .base import Notifier


class SlackNotifier(Notifier):
    """Posts a Slack message via an incoming webhook URL.

    Required env var: METAFLOW_HITL_SLACK_WEBHOOK
    Optional env var: METAFLOW_HITL_ARGO_UI_URL
    """

    def __init__(self, webhook_url=None):
        self.webhook_url = webhook_url or os.environ.get(
            "METAFLOW_HITL_SLACK_WEBHOOK"
        )
        if not self.webhook_url:
            raise RuntimeError(
                "METAFLOW_HITL_SLACK_WEBHOOK environment variable is required "
                "for the Slack notifier"
            )
        self.argo_ui_url = os.environ.get("METAFLOW_HITL_ARGO_UI_URL", "")

    def send(
        self,
        approval_id,
        flow_name,
        run_id,
        step_name,
        message,
        argo_workflow_name,
        argo_resume_cmd,
        expires_at,
    ):
        argo_ui_link = ""
        if self.argo_ui_url and argo_workflow_name:
            argo_ui_link = "\n*Argo UI:* <%s/workflows/argo/%s|View workflow>" % (
                self.argo_ui_url.rstrip("/"),
                argo_workflow_name,
            )

        text = (
            "*Human approval required* :raised_hand:\n"
            "*Flow:* `%s`\n"
            "*Run ID:* `%s`\n"
            "*Step:* `%s`\n"
            "*Message:* %s\n"
            "*Expires:* %s\n"
            "%s\n"
            "*Resume CLI:* `%s`\n"
            "*Reject:* `python flow.py hitl reject %s`"
        ) % (
            flow_name,
            run_id,
            step_name,
            message or "(none)",
            expires_at,
            argo_ui_link,
            argo_resume_cmd,
            approval_id,
        )

        payload = {"text": text}
        resp = requests.post(
            self.webhook_url,
            data=json.dumps(payload),
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
