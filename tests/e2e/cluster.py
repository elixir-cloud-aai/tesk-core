"""Tasks run by the taskmaster in a cluster, for the end-to-end tests."""

from collections.abc import Callable
import json
import os
import time
from typing import Any
import uuid

from kubernetes import client
from kubernetes.client.rest import ApiException
import pytest

# Namespace of the tests, with the manifests of `tests/e2e/manifests/`
NAMESPACE = os.environ.get("TESK_E2E_NAMESPACE", "default")
# Images loaded into the cluster's nodes (also named in `.github/workflows/
# ci.yml` and `tests/e2e/manifests/http-server.yaml`); the filer image also
# runs the executors and the HTTP server, so that the tests pull no other
# image
TASKMASTER_IMAGE = "tesk-core-taskmaster:e2e"
FILER_NAME = "tesk-core-filer"
FILER_TAG = "e2e"
FILER_IMAGE = f"{FILER_NAME}:{FILER_TAG}"
SERVICE_ACCOUNT = "tesk-e2e-taskmaster"
HTTP_URL = f"http://tesk-e2e-http.{NAMESPACE}.svc.cluster.local:8080"
HTTP_POD = "tesk-e2e-http"
# Seconds to wait for a task
TASK_TIMEOUT = 300


def executor(task_name: str, index: int, script: str, **resources: Any) -> Any:
    """Describe an executor job, as the TESK API converts it.

    Args:
        task_name: Name of the task (of its taskmaster job).
        index: Number of the executor.
        script: Shell script that the executor runs.
        **resources: The container's resources (e.g. `requests`).

    Returns:
        The job's manifest.
    """
    name = f"{task_name}-ex-{index:02d}"
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "labels": {
                "job-type": "executor",
                "taskmaster-name": task_name,
                "executor-no": str(index),
            },
        },
        "spec": {
            "template": {
                "metadata": {"name": name},
                "spec": {
                    "containers": [
                        {
                            "name": name,
                            "image": FILER_IMAGE,
                            "imagePullPolicy": "Never",
                            "command": ["sh", "-c", script],
                            "resources": resources,
                            # Writes to the task's volume, whose directories
                            # the filer (or the kubelet) created
                            "securityContext": {"runAsUser": 0},
                        }
                    ],
                    "restartPolicy": "Never",
                },
            }
        },
    }


class Task:
    """A task run by a taskmaster job in the cluster."""

    def __init__(
        self,
        batch: client.BatchV1Api,
        core: client.CoreV1Api,
        scripts: list[str],
        *,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
        env: dict[str, str] | None = None,
        resources: dict[str, Any] | None = None,
    ) -> None:
        """Describe the task and its taskmaster job; `submit` creates the job.

        Args:
            batch: The client of the batch API.
            core: The client of the core API.
            scripts: The shell script of each executor.
            inputs: The task's inputs.
            outputs: The task's outputs.
            env: The taskmaster's environment, as the TESK API passes it.
            resources: The resources of each executor's container.
        """
        self.batch = batch
        self.core = core
        self.name = f"task-{uuid.uuid4().hex[:8]}"
        self.data = {
            "executors": [
                executor(self.name, index, script, **(resources or {}))
                for index, script in enumerate(scripts)
            ],
            "inputs": inputs or [],
            "outputs": outputs or [],
            "volumes": [],
            "resources": {"disk_gb": 0.1},
        }
        container = {
            "name": "taskmaster",
            "image": TASKMASTER_IMAGE,
            "imagePullPolicy": "Never",
            "args": [
                json.dumps(self.data),
                "-n",
                NAMESPACE,
                "-fn",
                FILER_NAME,
                "-fv",
                FILER_TAG,
                "-pt",
                "120",
                "-d",
            ],
            "env": [{"name": k, "value": v} for k, v in (env or {}).items()],
            "volumeMounts": [
                {"name": "podinfo", "mountPath": "/podinfo", "readOnly": True}
            ],
        }
        self.body = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": self.name, "labels": {"job-type": "taskmaster"}},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "metadata": {"labels": {"job-type": "taskmaster"}},
                    "spec": {
                        "serviceAccountName": SERVICE_ACCOUNT,
                        "restartPolicy": "Never",
                        "containers": [container],
                        "volumes": [
                            {
                                "name": "podinfo",
                                "downwardAPI": {
                                    "items": [
                                        {
                                            "path": "labels",
                                            "fieldRef": {
                                                "fieldPath": "metadata.labels"
                                            },
                                        }
                                    ]
                                },
                            }
                        ],
                    },
                },
            },
        }

    def submit(self) -> None:
        """Create the taskmaster job of the task."""
        self.batch.create_namespaced_job(NAMESPACE, self.body)

    def job(self, suffix: str = "") -> Any:
        """Read a job of the task.

        Args:
            suffix: The suffix of the job's name, e.g. `-ex-00`; none for the
                taskmaster's job.

        Returns:
            The job, or None if it doesn't exist.
        """
        try:
            return self.batch.read_namespaced_job(self.name + suffix, NAMESPACE)
        except ApiException as ex:
            if ex.status == 404:
                return None
            raise

    def wait(self, predicate: Callable[[], bool], what: str) -> None:
        """Wait until a predicate holds, at most `TASK_TIMEOUT` seconds.

        Args:
            predicate: The predicate.
            what: What is waited for, for the error.
        """
        deadline = time.monotonic() + TASK_TIMEOUT
        while not predicate():
            if time.monotonic() > deadline:
                pytest.fail(f"Timed out waiting for {what}: {self.logs()}")
            time.sleep(2)

    def wait_finished(self) -> str:
        """Wait until the taskmaster's job has ended.

        Returns:
            The condition that ended it: `Complete` or `Failed`.
        """
        self.wait(
            lambda: (
                (job := self.job()) is not None
                and (condition(job, "Complete") or condition(job, "Failed"))
            ),
            f"taskmaster job {self.name}",
        )
        return "Complete" if condition(self.job(), "Complete") else "Failed"

    def pods(self, suffix: str = "") -> list[Any]:
        """List the pods of a job of the task.

        Args:
            suffix: The suffix of the job's name; none for the taskmaster's job.

        Returns:
            The pods.
        """
        return list(
            self.core.list_namespaced_pod(
                NAMESPACE, label_selector=f"job-name={self.name}{suffix}"
            ).items
        )

    def running(self, suffix: str) -> bool:
        """Tell whether a pod of a job of the task is running.

        Args:
            suffix: The suffix of the job's name.

        Returns:
            Whether one of its pods is in the phase `Running` (`active` of the
            job also counts pending pods).
        """
        return any(
            pod.status is not None and pod.status.phase == "Running"
            for pod in self.pods(suffix)
        )

    def logs(self) -> str:
        """Read the logs of the taskmaster's pods, for errors.

        Returns:
            The logs.
        """
        logs = []
        for pod in self.pods():
            try:
                logs.append(
                    self.core.read_namespaced_pod_log(pod.metadata.name, NAMESPACE)
                )
            except ApiException as ex:
                # E.g. a container that hasn't started
                logs.append(f"No log of pod {pod.metadata.name}: {ex.reason}")
        return "\n".join(logs)

    def gone(self, suffix: str) -> bool:
        """Tell whether a job of the task is deleted, or being deleted.

        Args:
            suffix: The suffix of the job's name.

        Returns:
            Whether it is gone.
        """
        job = self.job(suffix)
        return job is None or job.metadata.deletion_timestamp is not None

    def pvc_gone(self) -> bool:
        """Tell whether the task's PVC is deleted, or being deleted.

        Returns:
            Whether it is gone.
        """
        try:
            pvc = self.core.read_namespaced_persistent_volume_claim(
                self.name + "-pvc", NAMESPACE
            )
        except ApiException as ex:
            if ex.status == 404:
                return True
            raise
        return pvc.metadata.deletion_timestamp is not None

    def delete(self) -> None:
        """Delete the taskmaster's job, with it the task's jobs, and the PVC.

        The task's jobs are owned by the taskmaster's job, so Kubernetes
        deletes them with it. The PVC is deleted once the taskmaster's pods
        are gone (or after `TASK_TIMEOUT` seconds), so that a taskmaster that
        still runs can't create it afterwards. Objects that are already gone
        (or were never created) are skipped, so that a test that failed
        leaves nothing behind and its error isn't hidden.
        """
        try:
            self.batch.delete_namespaced_job(
                self.name,
                NAMESPACE,
                body=client.V1DeleteOptions(propagation_policy="Background"),
            )
        except ApiException as ex:
            if ex.status != 404:
                raise
        deadline = time.monotonic() + TASK_TIMEOUT
        while self.pods() and time.monotonic() < deadline:
            time.sleep(2)
        try:
            self.core.delete_namespaced_persistent_volume_claim(
                self.name + "-pvc", NAMESPACE
            )
        except ApiException as ex:
            if ex.status != 404:
                raise


def condition(job: Any, kind: str) -> bool:
    """Tell whether a job has a condition.

    Args:
        job: The job.
        kind: The type of the condition.

    Returns:
        Whether the condition is `True`; False for a job without a status yet.
    """
    conditions = (job.status.conditions if job.status else None) or []
    return any(c.type == kind and c.status == "True" for c in conditions)
