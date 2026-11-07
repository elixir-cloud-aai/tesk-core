"""Kubernetes jobs of a task: its executors and filers."""

from collections.abc import Callable
from datetime import datetime, timezone
import logging
import time
from typing import Any

from kubernetes import client
from kubernetes.client.rest import ApiException

from tesk_core.Util import pprint

logging.basicConfig(format="%(message)s", level=logging.INFO)

# Reasons why containers of a pending pod wait that don't resolve by
# waiting; pods that wait for other reasons (e.g., to be scheduled, or while
# a large image is pulled) are queued and keep waiting
CANNOT_START_REASONS = frozenset(
    {
        "CreateContainerConfigError",
        "ErrImagePull",
        "ImagePullBackOff",
        "InvalidImageName",
    }
)


def cannot_start(pod: client.V1Pod) -> client.V1ContainerStateWaiting | None:
    """Find a container of a pod that waits for a reason that won't resolve.

    Args:
        pod: The pod.

    Returns:
        The waiting state of the first such container, init containers
        included, or `None`.
    """
    statuses = (pod.status.init_container_statuses or []) + (
        pod.status.container_statuses or []
    )
    for status in statuses:
        waiting = status.state.waiting if status.state is not None else None
        if waiting is not None and waiting.reason in CANNOT_START_REASONS:
            return waiting
    return None


def pod_timeout_status(pod: client.V1Pod, legacy_check: bool) -> str:
    """Get the status of a job whose pending pod can't start.

    Args:
        pod: The pod, after the timeout.
        legacy_check: Whether the earlier pod check, which covered only
            started pods, would still have run.

    Returns:
        `Error` for a started pod whose first container can't pull its image
        (`ImagePullBackOff`), if the earlier check would have found it: the
        only case that timed out before, and whose executor job is deleted.
        Otherwise `PodTimeout`, and the job is kept.
    """
    statuses = pod.status.container_statuses or []
    state = statuses[0].state if statuses else None
    waiting = state.waiting if state is not None else None
    if (
        legacy_check
        and pod.status.start_time is not None
        and waiting is not None
        and waiting.reason == "ImagePullBackOff"
    ):
        return "Error"
    return "PodTimeout"


class Job:
    """A Kubernetes job, run to completion.

    Attributes:
        name: Name of the job.
        namespace: Namespace of the job.
        status: Last known status: `Initialized`, `Running`, `Complete`,
            `Failed`, `Error` or `PodTimeout`.
        bv1: Client of the Kubernetes batch API.
        cv1: Client of the Kubernetes core API.
        timeout: Seconds after which pending pods that can't start (see
            `CANNOT_START_REASONS`) fail the job.
        body: The manifest of the job.
    """

    def __init__(
        self, body: dict[str, Any], name: str = "task-job", namespace: str = "default"
    ) -> None:
        """Describe the job; `run_to_completion` runs it.

        Args:
            body: The manifest of the job; its name is set to `name`.
            name: Name of the job.
            namespace: Namespace of the job.
        """
        self.name = name
        self.namespace = namespace
        self.status = "Initialized"
        self.bv1 = client.BatchV1Api()
        self.cv1 = client.CoreV1Api()
        self.timeout = 240
        # Whether the earlier pod check, which stopped once no pending pod
        # had started, would have stopped
        self.legacy_check_stopped = False
        self.body = body
        self.body["metadata"]["name"] = self.name

    def run_to_completion(
        self,
        poll_interval: float,
        check_cancelled: Callable[[], bool],
        pod_timeout: int,
    ) -> str:
        """Create the job, or read it if it exists, and wait until it ends.

        Args:
            poll_interval: Seconds between status checks.
            check_cancelled: Tells whether the task was cancelled; if so, the
                job is deleted.
            pod_timeout: Seconds after which pending pods that can't start
                (see `CANNOT_START_REASONS`) fail the job.

        Returns:
            The final status: `Complete`, `Failed`, `Error`, `PodTimeout`
            or `Cancelled`.

        Raises:
            ApiException: The Kubernetes API returned an error other than a
                conflict when creating the job.
        """
        logging.debug("Creating job '%s'...", self.name)
        logging.debug(pprint(self.body))
        self.timeout = pod_timeout
        try:
            self.bv1.create_namespaced_job(self.namespace, self.body)
        except ApiException as ex:
            if ex.status == 409:
                logging.debug("Reading existing job: %s ", self.name)
                self.bv1.read_namespaced_job(self.name, self.namespace)
            else:
                logging.debug(ex.body)
                raise ApiException(ex.status, ex.reason) from ex
        is_all_pods_running = False
        status, is_all_pods_running = self.get_status(is_all_pods_running)
        while status == "Running":
            if check_cancelled():
                self.delete()
                return "Cancelled"
            time.sleep(poll_interval)
            status, is_all_pods_running = self.get_status(is_all_pods_running)
        # The job is kept after a pod timeout, so check for a cancellation
        # that came in since the last check
        if status == "PodTimeout" and check_cancelled():
            self.delete()
            return "Cancelled"
        return status

    def get_status(self, is_all_pods_runnning: bool) -> tuple[str, bool]:
        """Get the status of the job.

        Args:
            is_all_pods_runnning: Whether all pods of the job were found
                running (no longer pending) by an earlier call.

        Returns:
            The status (`Running`, `Complete`, `Failed`, `Error` or
            `PodTimeout`) and whether all pods of the job are running.
        """
        job = self.bv1.read_namespaced_job(self.name, self.namespace)
        try:
            # Loops around the status conditions array, and looks for
            # 'Complete', 'Failed' or 'SuccessCriteriaMet'. If none of these
            # are found, the Job is marked as 'Error'
            for condition in job.status.conditions:
                if condition.type == "Complete" and condition.status:
                    self.status = "Complete"
                    break
                if condition.type == "Failed" and condition.status:
                    self.status = "Failed"
                    break
                if condition.type == "SuccessCriteriaMet" and condition.status:
                    self.status = "Complete"
                    break
                self.status = "Error"
        except TypeError:
            # The condition is not initialized, so it is not complete yet,
            # wait for it
            self.status = "Running"
            job_duration = 0.0
            if job.status.active and job.status.start_time:
                job_duration = (
                    datetime.now(timezone.utc) - job.status.start_time
                ).total_seconds()
            if job_duration > self.timeout and not is_all_pods_runnning:
                pods = (
                    self.cv1.list_namespaced_pod(
                        self.namespace, label_selector=f"job-name={self.name}"
                    )
                ).items
                is_all_pods_runnning = True
                if not any(
                    pod.status.phase == "Pending" and pod.status.start_time
                    for pod in pods
                ):
                    self.legacy_check_stopped = True
                for pod in pods:
                    if pod.status.phase != "Pending":
                        continue
                    is_all_pods_runnning = False
                    since = pod.status.start_time or pod.metadata.creation_timestamp
                    if since is None:
                        continue
                    delta = (datetime.now(timezone.utc) - since).total_seconds()
                    waiting = cannot_start(pod)
                    if delta > self.timeout and waiting is not None:
                        logging.info(waiting)
                        status = pod_timeout_status(pod, not self.legacy_check_stopped)
                        return status, is_all_pods_runnning

        return self.status, is_all_pods_runnning

    def delete(self) -> None:
        """Delete the job and, in the background, its pods."""
        logging.info("Removing failed jobs")
        self.bv1.delete_namespaced_job(
            self.name,
            self.namespace,
            body=client.V1DeleteOptions(propagation_policy="Background"),
        )
