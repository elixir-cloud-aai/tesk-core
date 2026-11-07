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

# Job conditions that end a job, when their status is "True"
JOB_COMPLETE_CONDITIONS = frozenset({"Complete", "SuccessCriteriaMet"})
JOB_FAILED_CONDITIONS = frozenset({"Failed", "FailureTarget"})


class Job:
    """A Kubernetes job, run to completion.

    Attributes:
        name: Name of the job.
        namespace: Namespace of the job.
        status: Last known status: `Initialized`, `Running`, `Complete` or
            `Failed`.
        bv1: Client of the Kubernetes batch API.
        cv1: Client of the Kubernetes core API.
        timeout: Seconds after which pods that can't pull their image fail
            the job.
        failing: Whether the job was failed (`fail`) and waits for
            Kubernetes to mark it `Failed`.
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
        self.failing = False
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
            pod_timeout: Seconds after which pods that can't pull their
                image fail the job.

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
        return status

    def get_status(self, is_all_pods_runnning: bool) -> tuple[str, bool]:
        """Get the status of the job.

        A pod that can't pull its image (`ImagePullBackOff`) for longer than
        `timeout` fails the job (`fail`); the job runs until Kubernetes marks
        it `Failed`.

        Args:
            is_all_pods_runnning: Whether all pods of the job were found
                running (no longer pending) by an earlier call.

        Returns:
            The status (`Running`, `Complete` or `Failed`) and whether all
            pods of the job are running.
        """
        job = self.bv1.read_namespaced_job(self.name, self.namespace)
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
        if self.status != "Running" or self.failing:
            return self.status, is_all_pods_runnning

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
            for pod in pods:
                if pod.status.phase == "Pending" and pod.status.start_time:
                    is_all_pods_runnning = False
                    delta = (
                        datetime.now(timezone.utc) - pod.status.start_time
                    ).total_seconds()
                    if (
                        delta > self.timeout
                        and pod.status.container_statuses[0].state.waiting.reason
                        == "ImagePullBackOff"
                    ):
                        logging.info(pod.status.container_statuses[0].state.waiting)
                        self.fail()
                        break

        return self.status, is_all_pods_runnning

    def fail(self) -> None:
        """Fail the job, and keep it and its pods.

        Sets an active deadline that has passed, so that Kubernetes stops the
        job's pods and marks it `Failed` with the reason `DeadlineExceeded`.
        """
        logging.info("Failing job '%s': its pod can't start", self.name)
        self.bv1.patch_namespaced_job(
            self.name, self.namespace, {"spec": {"activeDeadlineSeconds": 1}}
        )
        self.failing = True

    def delete(self) -> None:
        """Delete the job and, in the background, its pods (on cancellation)."""
        logging.info("Deleting job '%s'", self.name)
        self.bv1.delete_namespaced_job(
            self.name,
            self.namespace,
            body=client.V1DeleteOptions(propagation_policy="Background"),
        )
