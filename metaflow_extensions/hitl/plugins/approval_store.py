import json
import os
import uuid
from datetime import datetime, timezone, timedelta


def _parse_duration(duration_str):
    """Parse duration string like '30m', '4h', '7d' into timedelta."""
    unit = duration_str[-1]
    value = int(duration_str[:-1])
    if unit == "m":
        return timedelta(minutes=value)
    elif unit == "h":
        return timedelta(hours=value)
    elif unit == "d":
        return timedelta(days=value)
    raise ValueError("Unknown duration unit '%s'. Use m, h, or d." % unit)


class ApprovalStore:
    """Abstract base for approval state storage."""

    @classmethod
    def from_environment(cls):
        datastore_type = os.environ.get("METAFLOW_DEFAULT_DATASTORE", "local")
        if datastore_type == "s3":
            return S3ApprovalStore()
        return LocalApprovalStore()

    def create(
        self,
        flow_name,
        run_id,
        step_name,
        message,
        approvers,
        input_schema,
        timeout,
        argo_workflow_name=None,
        argo_namespace="default",
    ):
        raise NotImplementedError

    def get(self, approval_id):
        raise NotImplementedError

    def approve(self, approval_id, hitl_input=None):
        raise NotImplementedError

    def reject(self, approval_id, reason=""):
        raise NotImplementedError

    def list_pending(self, flow_name=None):
        raise NotImplementedError

    def list_all(self, flow_name=None):
        raise NotImplementedError

    def _make_record(
        self,
        flow_name,
        run_id,
        step_name,
        message,
        approvers,
        input_schema,
        timeout,
        argo_workflow_name,
        argo_namespace,
    ):
        approval_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        expires_at = now + _parse_duration(timeout)
        return approval_id, {
            "approval_id": approval_id,
            "flow_name": flow_name,
            "run_id": run_id,
            "step_name": step_name,
            "status": "pending",
            "message": message,
            "approvers": approvers or [],
            "input_schema": input_schema,
            "hitl_input": None,
            "argo_workflow_name": argo_workflow_name,
            "argo_namespace": argo_namespace,
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
            "rejection_reason": None,
        }


class LocalApprovalStore(ApprovalStore):
    """File-based approval store for local development."""

    def __init__(self, base_dir=None):
        self.base_dir = base_dir or os.path.expanduser("~/.metaflow/hitl")
        os.makedirs(self.base_dir, exist_ok=True)

    def _path(self, flow_name, approval_id):
        flow_dir = os.path.join(self.base_dir, flow_name)
        os.makedirs(flow_dir, exist_ok=True)
        return os.path.join(flow_dir, "%s.json" % approval_id)

    def create(
        self,
        flow_name,
        run_id,
        step_name,
        message,
        approvers,
        input_schema,
        timeout,
        argo_workflow_name=None,
        argo_namespace="default",
    ):
        approval_id, record = self._make_record(
            flow_name,
            run_id,
            step_name,
            message,
            approvers,
            input_schema,
            timeout,
            argo_workflow_name,
            argo_namespace,
        )
        path = self._path(flow_name, approval_id)
        with open(path, "w") as f:
            json.dump(record, f, indent=2)
        return approval_id

    def get(self, approval_id):
        # Search across all flow directories
        for flow_dir in os.listdir(self.base_dir):
            path = os.path.join(self.base_dir, flow_dir, "%s.json" % approval_id)
            if os.path.exists(path):
                with open(path) as f:
                    return json.load(f)
        raise KeyError("Approval '%s' not found" % approval_id)

    def _update(self, approval_id, updates):
        record = self.get(approval_id)
        record.update(updates)
        path = self._path(record["flow_name"], approval_id)
        with open(path, "w") as f:
            json.dump(record, f, indent=2)

    def approve(self, approval_id, hitl_input=None):
        self._update(
            approval_id,
            {
                "status": "approved",
                "hitl_input": hitl_input or {},
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def reject(self, approval_id, reason=""):
        self._update(
            approval_id,
            {
                "status": "rejected",
                "rejection_reason": reason,
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _iter_dirs(self, flow_name=None):
        if flow_name:
            return [os.path.join(self.base_dir, flow_name)]
        return [
            os.path.join(self.base_dir, d)
            for d in os.listdir(self.base_dir)
            if os.path.isdir(os.path.join(self.base_dir, d))
        ]

    def list_pending(self, flow_name=None):
        results = []
        for d in self._iter_dirs(flow_name):
            if not os.path.isdir(d):
                continue
            for fname in os.listdir(d):
                if not fname.endswith(".json"):
                    continue
                with open(os.path.join(d, fname)) as f:
                    record = json.load(f)
                if record.get("status") == "pending":
                    results.append(record)
        return results

    def list_all(self, flow_name=None):
        results = []
        for d in self._iter_dirs(flow_name):
            if not os.path.isdir(d):
                continue
            for fname in os.listdir(d):
                if not fname.endswith(".json"):
                    continue
                with open(os.path.join(d, fname)) as f:
                    results.append(json.load(f))
        return results


class S3ApprovalStore(ApprovalStore):
    """S3-backed approval store (works with MinIO via METAFLOW_S3_ENDPOINT_URL)."""

    def __init__(self):
        from metaflow.metaflow_config import DATASTORE_SYSROOT_S3

        self.sysroot = (DATASTORE_SYSROOT_S3 or "").rstrip("/")
        if not self.sysroot:
            raise RuntimeError(
                "METAFLOW_DATASTORE_SYSROOT_S3 must be set for S3 approval store"
            )

    def _key(self, flow_name, approval_id):
        return "%s/hitl/%s/%s.json" % (self.sysroot, flow_name, approval_id)

    def _s3_put(self, key, data):
        import boto3

        s3 = self._s3_client()
        bucket, prefix = self._parse_s3_key(key)
        s3.put_object(Bucket=bucket, Key=prefix, Body=json.dumps(data).encode())

    def _s3_get(self, key):
        import boto3

        s3 = self._s3_client()
        bucket, prefix = self._parse_s3_key(key)
        resp = s3.get_object(Bucket=bucket, Key=prefix)
        return json.loads(resp["Body"].read())

    def _s3_client(self):
        import boto3

        endpoint = os.environ.get("METAFLOW_S3_ENDPOINT_URL")
        kwargs = {}
        if endpoint:
            kwargs["endpoint_url"] = endpoint
        return boto3.client("s3", **kwargs)

    def _parse_s3_key(self, full_key):
        # full_key: s3://bucket/prefix/...
        without_scheme = full_key[len("s3://"):]
        bucket, _, prefix = without_scheme.partition("/")
        return bucket, prefix

    def _list_keys(self, prefix):
        import boto3

        s3 = self._s3_client()
        bucket, s3_prefix = self._parse_s3_key(prefix)
        paginator = s3.get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                keys.append("s3://%s/%s" % (bucket, obj["Key"]))
        return keys

    def create(
        self,
        flow_name,
        run_id,
        step_name,
        message,
        approvers,
        input_schema,
        timeout,
        argo_workflow_name=None,
        argo_namespace="default",
    ):
        approval_id, record = self._make_record(
            flow_name,
            run_id,
            step_name,
            message,
            approvers,
            input_schema,
            timeout,
            argo_workflow_name,
            argo_namespace,
        )
        self._s3_put(self._key(flow_name, approval_id), record)
        return approval_id

    def get(self, approval_id):
        # We need to search; try all flow dirs by listing with partial prefix
        prefix = "%s/hitl/" % self.sysroot
        for key in self._list_keys(prefix):
            if approval_id in key:
                return self._s3_get(key)
        raise KeyError("Approval '%s' not found" % approval_id)

    def _update(self, approval_id, updates):
        record = self.get(approval_id)
        record.update(updates)
        key = self._key(record["flow_name"], approval_id)
        self._s3_put(key, record)

    def approve(self, approval_id, hitl_input=None):
        self._update(
            approval_id,
            {
                "status": "approved",
                "hitl_input": hitl_input or {},
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def reject(self, approval_id, reason=""):
        self._update(
            approval_id,
            {
                "status": "rejected",
                "rejection_reason": reason,
                "resolved_at": datetime.now(timezone.utc).isoformat(),
            },
        )

    def _hitl_prefix(self, flow_name=None):
        if flow_name:
            return "%s/hitl/%s/" % (self.sysroot, flow_name)
        return "%s/hitl/" % self.sysroot

    def list_pending(self, flow_name=None):
        results = []
        for key in self._list_keys(self._hitl_prefix(flow_name)):
            record = self._s3_get(key)
            if record.get("status") == "pending":
                results.append(record)
        return results

    def list_all(self, flow_name=None):
        return [
            self._s3_get(key)
            for key in self._list_keys(self._hitl_prefix(flow_name))
        ]
