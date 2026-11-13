"""Kubernetes jobs of a task: its executors and filers."""

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
import logging
import time
from typing import Any

from kubernetes import client
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

from tesk_core.Util import pprint

# Job conditions that end a job, when their status is "True"
JOB_COMPLETE_CONDITIONS = frozenset({"Complete", "SuccessCriteriaMet"})
JOB_FAILED_CONDITIONS = frozenset({"Failed", "FailureTarget"})
# Active deadline that fails a job (`Job.fail`), in seconds
FAIL_DEADLINE = 1
# Seconds to connect to and to wait for the Kubernetes API when deleting a
# job, per attempt, so that a stalled API server can't block the taskmaster
# (for a job whose pod wasn't scheduled in time, the next poll tries again)
DELETE_TIMEOUT = (5, 30)
# Why a job whose pod wasn't scheduled in time is failed, for the log
UNSCHEDULED_REASON = "its pod wasn't scheduled in time"
# Seconds between checks of a job's pending pods, so that queued tasks
# don't list their pods on every poll
POD_CHECK_INTERVAL = 60
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


def unschedulable(pod: client.V1Pod) -> str | None:
    """Tell why the scheduler can't place a pod, if it can't.

    Args:
        pod: The pod.

    Returns:
        The message of the pod's condition `PodScheduled`, if it is `False`
        with the reason `Unschedulable` (e.g. no node has enough CPU or
        memory), or `None`.
    """
    for condition in pod.status.conditions or []:
        if (
            condition.type == "PodScheduled"
            and condition.status == "False"
            and condition.reason == "Unschedulable"
        ):
            message: str = condition.message or "no reason given"
            return message
    return None


class Job:
    """A Kubernetes job, run to completion.

    Attributes:
        name: Name of the job.
        namespace: Namespace of the job.
        status: Last known status: `Initialized`, `Running`, `Complete` or
            `Failed`.
        bv1: Client of the Kubernetes batch API.
        cv1: Client of the Kubernetes core API.
        timeout: Seconds after which pending pods that can't start (see
            `CANNOT_START_REASONS`) fail the job.
        failing_since: When the job was failed (`fail`), to wait for
            Kubernetes to mark it `Failed`; `None` before.
        pod_check_interval: Seconds between checks of pending pods.
        next_pod_check: When pending pods are checked next; `None` before
            the first check.
        failed_pods: The number of the job's failed pods last seen; when it
            grows, the job's new pods are checked again.
        body: The manifest of the job.
        max_queue: Seconds after which a pod that the scheduler can't place
            (`unschedulable`) ends the job; `None` waits forever.
        deleting: Whether a deletion of `end_unschedulable` failed, and is
            tried again; a job that is gone then counts as deleted.
        delete_unschedulable: Whether such a job is deleted, rather than
            failed and kept (`fail`).
        ended_by_taskmaster: Whether the taskmaster ended the job, rather than
            its pods: it failed it (`fail`, also in an earlier run, as the job's
            `activeDeadlineSeconds` shows) or deleted it
            (`delete_unscheduled`), e.g. as its pod couldn't start or be
            scheduled in time. The job's failure then isn't the executor's
            own: its container may never have run.
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
        self.failing_since: datetime | None = None
        self.pod_check_interval = POD_CHECK_INTERVAL
        self.next_pod_check: datetime | None = None
        self.failed_pods = 0
        self.max_queue: int | None = None
        self.delete_unschedulable = False
        self.deleting = False
        self.ended_by_taskmaster = False
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
            The final status: `Complete`, `Failed` or `Cancelled`. Only a
            cancelled job is deleted.

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
                # Without the response body, which can repeat the manifest
                logging.debug(
                    "Can't create job '%s': %s %s", self.name, ex.status, ex.reason
                )
                raise ApiException(ex.status, ex.reason) from ex
        is_all_pods_running = False
        status, is_all_pods_running = self.get_status(is_all_pods_running)
        while status == "Running":
            if check_cancelled():
                self.delete_quietly()
                return "Cancelled"
            time.sleep(poll_interval)
            status, is_all_pods_running = self.get_status(is_all_pods_running)
        return status

    def get_status(self, is_all_pods_runnning: bool) -> tuple[str, bool]:
        """Get the status of the job.

        A pending pod that can't start (see `CANNOT_START_REASONS`) for
        longer than `timeout`, counted from its start or, if it hasn't
        started, its creation, fails the job (`fail`). With `max_queue`, a
        pod that the scheduler can't place (`unschedulable`) for longer than
        that, counted from its creation, ends the job
        (`end_unschedulable`). Pending pods are checked at most every
        `pod_check_interval` seconds, once the job has run for longer than
        `timeout` or `max_queue`. A failed job runs until Kubernetes marks it
        `Failed`, for at most another `timeout`; then it counts as `Failed`,
        and is kept.

        Args:
            is_all_pods_runnning: Whether all pods of the job were found
                running (no longer pending) by an earlier call.

        Returns:
            The status (`Running`, `Complete` or `Failed`) and whether all
            pods of the job are running.
        """
        # A deletion of `end_unschedulable` that failed is tried again first,
        # whatever the job and its pods do meanwhile
        if self.deleting:
            self.status = self.delete_unscheduled()
            return self.status, is_all_pods_runnning
        job = self.bv1.read_namespaced_job(self.name, self.namespace)
        spec = getattr(job, "spec", None)
        if getattr(spec, "active_deadline_seconds", None) == FAIL_DEADLINE:
            self.ended_by_taskmaster = True
        self.status = "Running"
        # Only conditions with the status "True" count. `FailureTarget`
        # (Kubernetes 1.31+) comes before `Failed`, `SuccessCriteriaMet`
        # before `Complete`; other conditions (e.g., `Suspended`) leave the
        # job running
        for condition in job.status.conditions or []:
            if condition.status != "True":
                continue
            if condition.type in JOB_COMPLETE_CONDITIONS:
                self.status = "Complete"
                break
            if condition.type in JOB_FAILED_CONDITIONS:
                self.status = "Failed"
                break
        if self.status != "Running":
            return self.status, is_all_pods_runnning
        # A pod failed, so the job may have created a new one (a retry, or
        # the replacement of an evicted pod): check the pods again
        failed_pods = job.status.failed or 0
        if failed_pods > self.failed_pods:
            self.failed_pods = failed_pods
            is_all_pods_runnning = False
        # A job that an earlier run of the taskmaster failed (see `fail`)
        if (
            self.failing_since is None
            and getattr(spec, "active_deadline_seconds", None) == FAIL_DEADLINE
        ):
            self.failing_since = datetime.now(timezone.utc)
        if self.failing_since is not None:
            waited = (datetime.now(timezone.utc) - self.failing_since).total_seconds()
            if waited > self.timeout:
                logging.warning(
                    "Job '%s' wasn't marked failed within %s s", self.name, self.timeout
                )
                self.status = "Failed"
            return self.status, is_all_pods_runnning

        job_duration = 0.0
        if job.status.active and job.status.start_time:
            job_duration = (
                datetime.now(timezone.utc) - job.status.start_time
            ).total_seconds()
        now = datetime.now(timezone.utc)
        check_after = self.timeout
        if self.max_queue is not None:
            check_after = min(check_after, self.max_queue)
        if (
            job_duration > check_after
            and not is_all_pods_runnning
            and (self.next_pod_check is None or now >= self.next_pod_check)
        ):
            pods = (
                self.cv1.list_namespaced_pod(
                    self.namespace, label_selector=f"job-name={self.name}"
                )
            ).items
            is_all_pods_runnning = True
            failed = False
            # Whether a pod of the job ran (or runs), so that its container
            # ran and its logs are kept
            ran = any(pod.status.phase != "Pending" for pod in pods)
            for pod in pods:
                if pod.status.phase != "Pending":
                    continue
                # Also pods that aren't scheduled yet, so that an image pull
                # error after scheduling is still found
                is_all_pods_runnning = False
                since = pod.status.start_time or pod.metadata.creation_timestamp
                if since is None:
                    continue
                delta = (datetime.now(timezone.utc) - since).total_seconds()
                # Only with a max queue time, and only once it has passed
                if (
                    self.max_queue is not None
                    and delta > self.max_queue
                    and (scheduling := unschedulable(pod)) is not None
                ):
                    logging.error(
                        "Pod '%s' of job '%s' wasn't scheduled within %s s: %s",
                        pod.metadata.name,
                        self.name,
                        self.max_queue,
                        scheduling,
                    )
                    self.status = self.end_unschedulable(ran=ran)
                    failed = True
                    break
                waiting = cannot_start(pod)
                if delta > self.timeout and waiting is not None:
                    logging.info(
                        "Pod '%s' of job '%s' can't start: %s",
                        pod.metadata.name,
                        self.name,
                        waiting,
                    )
                    self.status = self.fail()
                    failed = True
                    break
            # Not after trying to fail the job: after an error, the next poll
            # tries again
            if not is_all_pods_runnning and not failed:
                self.next_pod_check = now + timedelta(seconds=self.pod_check_interval)

        return self.status, is_all_pods_runnning

    def end_unschedulable(self, ran: bool = False) -> str:
        """End the job, as its pod can't be scheduled in time.

        Deletes it (`delete_unscheduled`) if `delete_unschedulable` and none
        of its pods ran or failed before (so its container never ran);
        otherwise fails it (`fail`), which keeps the logs of pods that ran.

        Args:
            ran: Whether a pod of the job ran, or runs.

        Returns:
            The job's status, as `delete_unscheduled` or `fail`.
        """
        if not self.delete_unschedulable or self.failed_pods or ran:
            return self.fail(UNSCHEDULED_REASON)
        return self.delete_unscheduled()

    def delete_unscheduled(self) -> str:
        """Delete the job whose pod wasn't scheduled in time.

        If the Kubernetes API rejects the deletion (a client error), fails
        the job instead (`fail`). After other errors, including a request
        that times out (`DELETE_TIMEOUT`), the next poll tries again
        (`deleting`), so that the job can't run after the task stopped.

        Returns:
            The job's status: `Failed` once deleted (or already gone),
            `Running` to try again, otherwise as `fail`.
        """
        self.ended_by_taskmaster = True
        try:
            self.delete(request_timeout=DELETE_TIMEOUT)
        except ApiException as ex:
            if ex.status == 404:
                return "Failed"
            if ex.status is not None and 400 <= ex.status < 500:
                return self.fail(UNSCHEDULED_REASON)
            logging.warning("Can't delete job '%s' (%s) yet", self.name, ex.status)
            self.deleting = True
            return "Running"
        except HTTPError as ex:
            logging.warning("Can't delete job '%s' (%s) yet", self.name, ex)
            self.deleting = True
            return "Running"
        return "Failed"

    def fail(self, reason: str = "its pod can't start") -> str:
        """Fail the job, and keep it.

        Sets an active deadline that has passed (`FAIL_DEADLINE`), so that
        Kubernetes deletes the job's active pods and marks the job `Failed`
        with the reason `DeadlineExceeded`. If the Kubernetes API rejects the
        change (a client error, e.g. a role without `patch` on jobs), the job
        is deleted instead, as it can't be left waiting; after other errors
        (e.g. server errors or no response), the next poll tries again.

        Args:
            reason: Why the job is failed, for the log.

        Returns:
            The job's status: `Running` while Kubernetes fails it, or the
            next poll tries again; `Failed` if the change was rejected.
        """
        self.ended_by_taskmaster = True
        logging.info("Failing job '%s': %s", self.name, reason)
        try:
            self.bv1.patch_namespaced_job(
                self.name,
                self.namespace,
                {"spec": {"activeDeadlineSeconds": FAIL_DEADLINE}},
            )
        except ApiException as ex:
            if ex.status is not None and 400 <= ex.status < 500:
                logging.error(
                    "Can't fail job '%s' (%s); deleting it", self.name, ex.status
                )
                self.delete_quietly()
                return "Failed"
            logging.warning("Can't fail job '%s' (%s) yet", self.name, ex.status)
            return "Running"
        self.failing_since = datetime.now(timezone.utc)
        return "Running"

    def delete_quietly(self) -> None:
        """Delete the job, with a timeout; log an error if that fails.

        On cancellation, on an interrupt of the taskmaster, and if the
        Kubernetes API rejects failing the job (`fail`); see
        `delete_job_quietly`.
        """
        _quietly(self.name, lambda: self.delete(request_timeout=DELETE_TIMEOUT))

    def delete(self, request_timeout: tuple[float, float] | None = None) -> None:
        """Delete the job and, in the background, its pods.

        Only on cancellation, on an interrupt of the taskmaster, if the
        Kubernetes API rejects failing it (`fail`), and for an executor whose
        pod wasn't scheduled in time (`delete_unscheduled`).

        Args:
            request_timeout: Seconds to connect and to wait for the
                Kubernetes API, per attempt; none by default.
        """
        delete_job(self.bv1, self.name, self.namespace, request_timeout)


def delete_job(
    bv1: client.BatchV1Api,
    name: str,
    namespace: str,
    request_timeout: tuple[float, float] | None = None,
) -> None:
    """Delete a job and, in the background, its pods.

    Args:
        bv1: The client of the batch API.
        name: Name of the job.
        namespace: Namespace of the job.
        request_timeout: Seconds to connect and to wait for the Kubernetes
            API, per attempt; none by default.
    """
    logging.info("Deleting job '%s'", name)
    bv1.delete_namespaced_job(
        name,
        namespace,
        body=client.V1DeleteOptions(propagation_policy="Background"),
        _request_timeout=request_timeout,
    )


def delete_job_quietly(bv1: client.BatchV1Api, name: str, namespace: str) -> None:
    """Delete a job (`delete_job`) with a timeout; log an error if that fails.

    A job that is already gone counts as deleted. Other errors, including a
    request that times out (`DELETE_TIMEOUT`), are logged, not raised, so
    that the taskmaster goes on (e.g. deletes the task's PVC and stops).

    Args:
        bv1: The client of the batch API.
        name: Name of the job.
        namespace: Namespace of the job.
    """
    _quietly(name, lambda: delete_job(bv1, name, namespace, DELETE_TIMEOUT))


def _quietly(name: str, delete: Callable[[], None]) -> None:
    """Delete a job; a job that is gone is no error, others are logged.

    Args:
        name: Name of the job, for the log.
        delete: Deletes the job.
    """
    try:
        delete()
    except ApiException as ex:
        if ex.status == 404:
            logging.debug("Job '%s' is already gone", name)
            return
        reason = " ".join(str(part) for part in (ex.status, ex.reason) if part)
        logging.error("Can't delete job '%s' (%s)", name, reason)
    except HTTPError as ex:
        logging.error("Can't delete job '%s' (%s)", name, ex)
