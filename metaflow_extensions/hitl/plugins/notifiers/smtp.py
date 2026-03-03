import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from .base import Notifier


class SmtpNotifier(Notifier):
    """Sends approval notification emails via SMTP.

    Required env vars:
        METAFLOW_HITL_SMTP_HOST
        METAFLOW_HITL_SMTP_FROM
    Optional env vars:
        METAFLOW_HITL_SMTP_PORT      (default: 587)
        METAFLOW_HITL_SMTP_USER
        METAFLOW_HITL_SMTP_PASSWORD
        METAFLOW_HITL_ARGO_UI_URL
    """

    def __init__(self):
        self.host = os.environ.get("METAFLOW_HITL_SMTP_HOST")
        self.port = int(os.environ.get("METAFLOW_HITL_SMTP_PORT", "587"))
        self.user = os.environ.get("METAFLOW_HITL_SMTP_USER")
        self.password = os.environ.get("METAFLOW_HITL_SMTP_PASSWORD")
        self.from_addr = os.environ.get("METAFLOW_HITL_SMTP_FROM")
        self.argo_ui_url = os.environ.get("METAFLOW_HITL_ARGO_UI_URL", "")

        if not self.host:
            raise RuntimeError(
                "METAFLOW_HITL_SMTP_HOST environment variable is required "
                "for the SMTP notifier"
            )
        if not self.from_addr:
            raise RuntimeError(
                "METAFLOW_HITL_SMTP_FROM environment variable is required "
                "for the SMTP notifier"
            )

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
        approvers=None,
    ):
        if not approvers:
            return

        argo_ui_link = ""
        if self.argo_ui_url and argo_workflow_name:
            argo_ui_link = (
                "<p><b>Argo UI:</b> <a href='%s/workflows/argo/%s'>View workflow</a></p>"
                % (self.argo_ui_url.rstrip("/"), argo_workflow_name)
            )

        html_body = """
<html><body>
<h2>Human approval required</h2>
<table>
  <tr><td><b>Flow</b></td><td>{flow_name}</td></tr>
  <tr><td><b>Run ID</b></td><td>{run_id}</td></tr>
  <tr><td><b>Step</b></td><td>{step_name}</td></tr>
  <tr><td><b>Message</b></td><td>{message}</td></tr>
  <tr><td><b>Expires</b></td><td>{expires_at}</td></tr>
</table>
{argo_ui_link}
<p><b>Resume CLI:</b> <code>{argo_resume_cmd}</code></p>
<p><b>Reject:</b> <code>python flow.py hitl reject {approval_id}</code></p>
</body></html>
""".format(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            message=message or "(none)",
            expires_at=expires_at,
            argo_ui_link=argo_ui_link,
            argo_resume_cmd=argo_resume_cmd,
            approval_id=approval_id,
        )

        plain_body = (
            "Human approval required\n"
            "Flow: {flow_name}\n"
            "Run ID: {run_id}\n"
            "Step: {step_name}\n"
            "Message: {message}\n"
            "Expires: {expires_at}\n"
            "Resume CLI: {argo_resume_cmd}\n"
            "Reject: python flow.py hitl reject {approval_id}\n"
        ).format(
            flow_name=flow_name,
            run_id=run_id,
            step_name=step_name,
            message=message or "(none)",
            expires_at=expires_at,
            argo_resume_cmd=argo_resume_cmd,
            approval_id=approval_id,
        )

        msg = MIMEMultipart("alternative")
        msg["Subject"] = "[HITL] Approval required: %s/%s/%s" % (
            flow_name,
            run_id,
            step_name,
        )
        msg["From"] = self.from_addr
        msg["To"] = ", ".join(approvers)
        msg.attach(MIMEText(plain_body, "plain"))
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(self.host, self.port) as smtp:
            smtp.ehlo()
            smtp.starttls()
            if self.user and self.password:
                smtp.login(self.user, self.password)
            smtp.sendmail(self.from_addr, approvers, msg.as_string())
