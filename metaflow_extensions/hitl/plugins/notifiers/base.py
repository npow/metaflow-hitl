from abc import ABC, abstractmethod


class Notifier(ABC):
    @abstractmethod
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
        """Send notification for a pending approval.

        Parameters
        ----------
        approval_id : str
        flow_name : str
        run_id : str
        step_name : str
        message : str
            Human-readable message from the @hitl decorator.
        argo_workflow_name : str or None
        argo_resume_cmd : str
            e.g. "argo -n argo resume my-workflow-abc123"
        expires_at : str
            ISO 8601 datetime string.
        """
        ...
