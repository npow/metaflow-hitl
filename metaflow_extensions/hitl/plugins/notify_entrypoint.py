"""
Notification entrypoint — runs inside the injected hitl-notify-<step> container.

Usage
-----
python -m metaflow_extensions.hitl.plugins.notify_entrypoint \
  --flow-name MyFlow \
  --run-id argo-my-flow-abc123 \
  --step-name gate \
  --argo-workflow-name argo-my-flow-abc123 \
  --approvers "alice@co.com,bob@co.com" \
  --message "Review before deploy" \
  --timeout 24h \
  --notifier apprise

Apprise URLs can be passed via --apprise-urls or the METAFLOW_HITL_APPRISE_URLS
environment variable (comma- or newline-separated).
"""

import argparse
import os
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Create approval record and send HITL notification"
    )
    parser.add_argument("--flow-name", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--step-name", required=True)
    parser.add_argument("--argo-workflow-name", default=None)
    parser.add_argument("--argo-namespace", default="default")
    parser.add_argument("--approvers", default="")
    parser.add_argument("--message", default="")
    parser.add_argument("--timeout", default="24h")
    parser.add_argument("--notifier", default=None)
    parser.add_argument(
        "--apprise-urls",
        default=None,
        help="Comma-separated Apprise service URLs (overrides METAFLOW_HITL_APPRISE_URLS)",
    )
    parser.add_argument("--input-schema", default=None)
    args = parser.parse_args()

    from .approval_store import ApprovalStore

    store = ApprovalStore.from_environment()

    import json as _json

    input_schema = None
    if args.input_schema:
        try:
            input_schema = _json.loads(args.input_schema)
        except Exception:
            pass

    approvers = [a.strip() for a in args.approvers.split(",") if a.strip()]

    approval_id = store.create(
        flow_name=args.flow_name,
        run_id=args.run_id,
        step_name=args.step_name,
        message=args.message,
        approvers=approvers,
        input_schema=input_schema,
        timeout=args.timeout,
        argo_workflow_name=args.argo_workflow_name,
        argo_namespace=args.argo_namespace,
    )

    print("Created approval record: %s" % approval_id)

    if args.notifier:
        record = store.get(approval_id)
        argo_namespace = args.argo_namespace or "default"
        argo_workflow_name = args.argo_workflow_name
        if argo_workflow_name:
            argo_resume_cmd = "argo -n %s resume %s" % (
                argo_namespace,
                argo_workflow_name,
            )
        else:
            argo_resume_cmd = "n/a"

        if args.notifier == "slack":
            from .notifiers.slack import SlackNotifier

            notifier = SlackNotifier()
        elif args.notifier == "smtp":
            from .notifiers.smtp import SmtpNotifier

            notifier = SmtpNotifier()
        elif args.notifier == "apprise":
            from .notifiers.apprise_notifier import AppriseNotifier

            urls = None
            if args.apprise_urls:
                urls = [u.strip() for u in args.apprise_urls.split(",") if u.strip()]
            notifier = AppriseNotifier(urls=urls)
        else:
            print("Unknown notifier '%s', skipping notification" % args.notifier)
            sys.exit(0)

        notifier.send(
            approval_id=approval_id,
            flow_name=args.flow_name,
            run_id=args.run_id,
            step_name=args.step_name,
            message=args.message,
            argo_workflow_name=argo_workflow_name,
            argo_resume_cmd=argo_resume_cmd,
            expires_at=record["expires_at"],
        )
        print("Notification sent via %s" % args.notifier)

    sys.exit(0)


if __name__ == "__main__":
    main()
