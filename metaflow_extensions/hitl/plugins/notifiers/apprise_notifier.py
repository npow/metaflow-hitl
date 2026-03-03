"""Apprise-backed notifier — supports 100+ services via a single URL scheme."""

import os

from .base import Notifier


class AppriseNotifier(Notifier):
    """Send HITL notifications via any Apprise-supported service.

    Parameters
    ----------
    urls : list[str], optional
        Apprise service URLs, e.g.::

            ["slack://xoxb-token/C123CHANNEL",
             "mailto://user:pass@smtp.example.com",
             "pagerduty://apikey@routingkey"]

        If ``None``, reads the ``METAFLOW_HITL_APPRISE_URLS`` environment
        variable (newline- or comma-separated).

    Examples
    --------
    From the decorator::

        @hitl(
            notifier=["slack://xoxb-token/C123CHANNEL"],
            ...
        )

    Via environment variable::

        export METAFLOW_HITL_APPRISE_URLS="slack://xoxb-token/C123CHANNEL"

        @hitl(notifier="apprise", ...)
    """

    def __init__(self, urls=None):
        if urls is None:
            raw = os.environ.get("METAFLOW_HITL_APPRISE_URLS", "")
            urls = [u.strip() for u in raw.replace(",", "\n").splitlines() if u.strip()]
        self._urls = urls

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
        import apprise

        ap = apprise.Apprise()
        for url in self._urls:
            ap.add(url)

        title = "HITL Approval Required — %s / %s" % (flow_name, step_name)

        lines = []
        if message:
            lines += [message, ""]
        lines += [
            "Flow:    %s" % flow_name,
            "Run:     %s" % run_id,
            "Step:    %s" % step_name,
            "ID:      %s" % approval_id,
            "Resume:  %s" % argo_resume_cmd,
            "Expires: %s" % expires_at,
        ]

        ap.notify(title=title, body="\n".join(lines))
