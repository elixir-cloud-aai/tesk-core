#!/usr/bin/env python3
"""The taskmaster: runs a TES task as Kubernetes jobs.

Creates the task's persistent volume claim, runs a filer job for the inputs,
if any, one job per executor, one after another, and a filer job for the
outputs, if any.
"""

import argparse
import gzip
import json
import logging
import math
import os
import re
import signal
import sys
from types import FrameType
from typing import Any, NoReturn

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from urllib3.exceptions import HTTPError

from tesk_core.filer_class import Filer
from tesk_core.job import (
    JOB_COMPLETE_CONDITIONS,
    JOB_FAILED_CONDITIONS,
    Job,
    delete_job_quietly,
)
from tesk_core.pvc import PVC
from tesk_core.Util import configure_logging

created_jobs: list[Job] = []
created_pvc: PVC | None = None
# Seconds between polls of a job; set by `main` from `--poll-interval`,
# within limits, so that polls don't flood the Kubernetes API and the
# taskmaster still notices a cancellation
poll_interval: float = 5
min_poll_interval = 0.1
max_poll_interval = 3600.0
# Exit codes after `SIGINT` and `SIGTERM` (128 plus the signal's number, as
# a shell reports them), so that Kubernetes counts the pod as failed
sigint_exit_code = 130
sigterm_exit_code = 143
task_volume_basename = "task-volume"
# The labels of the taskmaster's pod (Downward API volume of the TESK API's
# taskmaster job)
pod_labels_file = "/podinfo/labels"
# The label and value with which the TESK API cancels a task
cancel_label = "task-status"
cancel_value = "Cancelled"
# The labels with the name of the job that created a pod: the first since
# Kubernetes 1.27, the second also before
job_name_labels = ("batch.kubernetes.io/job-name", "job-name")
# The labels with the UID of that job, likewise
job_uid_labels = ("batch.kubernetes.io/controller-uid", "controller-uid")
# Reads the taskmaster's job; created at the first check, after the
# Kubernetes configuration is loaded, and reused at every poll
batch_api: client.BatchV1Api | None = None
# Whether an error reading the job was logged at `ERROR` (shown without
# `--debug`); later ones are logged at `DEBUG`, not at every poll
job_read_error_logged = False
# Seconds to connect to and to wait for the Kubernetes API when reading the
# job, per attempt (the client tries up to 4 times), so that a stalled API
# server can't block the cancellation check for long
job_read_timeout = (5, 10)
# Seconds to connect to and to wait for the Kubernetes API when deleting the
# claim, per attempt, so that a stalled API server can't keep the taskmaster
# from ending
pvc_delete_timeout = (5, 30)
# The setting that keeps the claim of a task that doesn't complete (`true`)
keep_failed_pvc_env = "KEEP_FAILED_PVC"
# The setting with the seconds after which Kubernetes deletes the
# taskmaster's finished job, and with it the task's jobs; unset, no TTL
task_ttl_env = "TASK_TTL_SECONDS"
# The largest number of seconds of the settings `TASK_TTL_SECONDS` and
# `TESK_MAX_QUEUE_SECONDS`: the largest TTL that Kubernetes accepts (an
# int32), for both
max_seconds = 2**31 - 1
# The settings with the number of retries of each executor's and each filer's
# job (`backoffLimit`, an int32 too)
executor_backoff_env = "EXECUTOR_BACKOFF_LIMIT"
filer_backoff_env = "FILER_BACKOFF_LIMIT"
# The setting with the seconds after which a pod that can't be scheduled
# ends the task (`SYSTEM_ERROR`); unset, pods wait to be scheduled forever
max_queue_env = "TESK_MAX_QUEUE_SECONDS"
# The max queue time of the task's jobs; set by `main`
max_queue: int | None = None
# References to the taskmaster's job, which owns the jobs that it creates;
# set by `init_own_job`, none if the job can't be read
owner_references: list[dict[str, str]] = []
# Set by `main`
args = argparse.Namespace()
logger = logging.getLogger(__name__)


def run_executor(
    executor: dict[str, Any], namespace: str, pvc: PVC | None = None
) -> None:
    """Run an executor job to completion.

    Exits (`exit_cancelled`) if the job doesn't complete. A job that failed
    is kept, so that the TESK API reports the executor's state
    (`EXECUTOR_ERROR`) and logs. Executor jobs aren't retried
    (`backoffLimit: 0`), unless the job or `EXECUTOR_BACKOFF_LIMIT` sets a
    limit; that limit also covers pods that are evicted.

    Args:
        executor: The manifest of the executor job.
        namespace: Namespace of the job.
        pvc: The task's persistent volume claim, if any, mounted into the
            executor's first container.
    """
    jobname = executor["metadata"]["name"]
    spec = executor["spec"]["template"]["spec"]

    # A failed executor isn't retried, unless configured: a retry could hide
    # the failure and its logs
    backoff_limit = env_whole_number(
        executor_backoff_env, 0, "using the job's limit (0 if none)", "a whole number"
    )
    if backoff_limit is None:
        backoff_limit = executor["spec"].get("backoffLimit")
    executor["spec"]["backoffLimit"] = backoff_limit or 0

    if "restartPolicy" not in spec and "restart_policy" in spec:
        spec["restartPolicy"] = spec["restart_policy"]

    for container in spec["containers"]:
        if "limits" not in container["resources"]:
            container["resources"]["limits"] = None
        if container["resources"]["limits"] is None and (
            "requests" in container["resources"]
            and container["resources"]["requests"] is not None
        ):
            container["resources"]["limits"] = container["resources"]["requests"]

    if pvc is not None:
        mounts = spec["containers"][0].setdefault("volumeMounts", [])
        mounts.extend(pvc.volume_mounts)
        spec.setdefault("volumes", [])
        if spec["volumes"] is None:
            spec["volumes"] = []
            # volumes is a refence to spec['volumes']
            # This makes sure the next line does not fail if volumes was
            # originaly "null"
        spec["volumes"].extend(
            [
                {
                    "name": task_volume_basename,
                    "persistentVolumeClaim": {"readonly": False, "claimName": pvc.name},
                }
            ]
        )
    logger.debug("Created job: %s", jobname)
    # `Job.run_to_completion` logs the manifest, redacted
    # Deleted rather than failed if it can't be scheduled in time, so that the
    # TESK API reports `SYSTEM_ERROR` (a missing executor), not
    # `EXECUTOR_ERROR`: the executor never ran
    job = new_job(executor, jobname, namespace, delete_unschedulable=True)

    created_jobs.append(job)

    status = job.run_to_completion(poll_interval, check_cancelled, args.pod_timeout)
    if status != "Complete":
        exit_cancelled("Got status " + status)


# TODO move this code to PVC class


def append_mount(
    volume_mounts: list[dict[str, Any]], name: str, path: str, pvc: PVC
) -> None:
    """Add a volume mount on a new subpath of the claim, unless the path has one.

    Args:
        volume_mounts: The volume mounts, changed in place.
        name: Name of the volume.
        path: The mount path.
        pvc: The claim.
    """
    # Checks all mount paths in volume_mounts if the path given is already in
    # there
    duplicate = next(
        (mount for mount in volume_mounts if mount["mountPath"] == path), None
    )
    # If not, add mount path
    if duplicate is None:
        subpath = pvc.get_subpath()
        logger.debug(
            " ".join(
                ["appending" + name + "at path" + path + "with subPath:" + subpath]
            )
        )
        volume_mounts.append({"name": name, "mountPath": path, "subPath": subpath})


def dirname(iodata: dict[str, Any]) -> str:
    """Get the directory to mount for an input or output.

    Args:
        iodata: The input or output.

    Returns:
        The directory of a file, or the directory itself.
    """
    if iodata["type"] == "FILE":
        # strip filename from path
        r = "(.*)/"
        # Paths without `/` don't match and raise `AttributeError`, as before
        dirname: str = re.match(r, iodata["path"]).group(1)  # type: ignore[union-attr]
        logger.debug("dirname of %sis: %s", iodata["path"], dirname)
    elif iodata["type"] == "DIRECTORY":
        dirname = iodata["path"]

    return dirname


def generate_mounts(data: dict[str, Any], pvc: PVC) -> list[dict[str, Any]]:
    """Get the volume mounts of a task's volumes, inputs and outputs.

    Args:
        data: The task.
        pvc: The task's persistent volume claim.

    Returns:
        The volume mounts, one per path, each on a subpath of the claim.
    """
    volume_mounts: list[dict[str, Any]] = []

    # gather volumes that need to be mounted, without duplicates
    volume_name = task_volume_basename
    for volume in data["volumes"]:
        append_mount(volume_mounts, volume_name, volume, pvc)

    # gather other paths that need to be mounted from inputs/outputs FILE and
    # DIRECTORY entries
    for aninput in data["inputs"]:
        dirnm = dirname(aninput)
        append_mount(volume_mounts, volume_name, dirnm, pvc)

    for anoutput in data["outputs"]:
        dirnm = dirname(anoutput)
        append_mount(volume_mounts, volume_name, dirnm, pvc)

    return volume_mounts


def new_pvc(data: dict[str, Any]) -> PVC:
    """Describe the task's claim, `<task>-pvc` of the task's disk size.

    Args:
        data: The task.

    Returns:
        The claim, not created yet.
    """
    task_name = data["executors"][0]["metadata"]["labels"]["taskmaster-name"]
    return PVC(task_name + "-pvc", data["resources"]["disk_gb"], args.namespace)


def task_pvc(data: dict[str, Any]) -> PVC | None:
    """Describe the task's claim, if it has one, without creating it.

    Args:
        data: The task.

    Returns:
        The claim, as `init_pvc` creates it (`new_pvc`); none if the task
        has no volumes, inputs and outputs, and so no claim.
    """
    if not (data.get("volumes") or data.get("inputs") or data.get("outputs")):
        return None
    return new_pvc(data)


def init_pvc(data: dict[str, Any], filer: Filer) -> PVC:
    """Create the task's persistent volume claim and stage the inputs.

    The inputs filer job runs only if the task has inputs. Exits
    (`exit_cancelled`) if it doesn't complete.

    Args:
        data: The task.
        filer: The filer job, which gets the claim mounted.

    Returns:
        The claim.
    """
    global created_pvc  # noqa: PLW0603 -- module state, see `clean_on_interrupt`

    task_name = data["executors"][0]["metadata"]["labels"]["taskmaster-name"]
    pvc = new_pvc(data)

    mounts = generate_mounts(data, pvc)
    logging.debug(mounts)
    logging.debug(type(mounts))
    pvc.set_volume_mounts(mounts)
    filer.add_volume_mount(pvc)

    pvc.create()
    # to global var for cleanup purposes
    created_pvc = pvc

    if os.environ.get("NETRC_SECRET_NAME") is not None:
        filer.add_netrc_mount(os.environ.get("NETRC_SECRET_NAME"))

    # Volumes only need the claim: the subpaths of their mounts are created
    # when the executors start
    if not data["inputs"]:
        return pvc

    filerjob = new_job(
        filer.get_spec("inputs", args.debug),
        task_name + "-inputs-filer",
        args.namespace,
    )

    created_jobs.append(filerjob)
    # filerjob.run_to_completion(poll_interval)
    status = filerjob.run_to_completion(
        poll_interval, check_cancelled, args.pod_timeout
    )
    if status != "Complete":
        exit_cancelled("Got status " + status)

    return pvc


def run_task(
    data: dict[str, Any],
    filer_name: str,
    filer_version: str,
    have_json_pvc: bool = False,
) -> None:
    """Run a task: stage the inputs, run the executors, stage the outputs.

    Without volumes, inputs and outputs, no filer jobs and claim are created.
    The inputs filer runs only with inputs, the outputs filer only with
    outputs.

    Args:
        data: The task.
        filer_name: Image of the filer, without tag.
        filer_version: Tag of the filer image.
        have_json_pvc: Whether the task is in a ConfigMap named after it,
            rather than passed to the filer in the environment.
    """
    task_name = data["executors"][0]["metadata"]["labels"]["taskmaster-name"]
    pvc = None

    json_pvc = task_name if have_json_pvc else None

    if data["volumes"] or data["inputs"] or data["outputs"]:
        filer = Filer(
            task_name + "-filer",
            data,
            filer_name,
            filer_version,
            args.pull_policy_always,
            json_pvc,
        )

        if os.environ.get("TESK_FTP_USERNAME") is not None:
            filer.set_ftp(
                os.environ["TESK_FTP_USERNAME"], os.environ["TESK_FTP_PASSWORD"]
            )

        backoff_limit = env_whole_number(
            filer_backoff_env, 0, "using Kubernetes' default", "a whole number"
        )
        if backoff_limit is not None:
            filer.set_backoffLimit(backoff_limit)

        pvc = init_pvc(data, filer)

    # A job that completes between two polls isn't checked for cancellation:
    # check before each next job, so that a cancelled task starts no more
    for executor in data["executors"]:
        if check_cancelled():
            cancel_task(
                data, "Cancelled before executor " + executor["metadata"]["name"]
            )
        run_executor(executor, args.namespace, pvc)

    # run executors
    logging.debug("Finished running executors")

    # A task cancelled after its last executor stops as cancelled (which
    # keeps its claim with `KEEP_FAILED_PVC`), with outputs or not
    if pvc is not None and check_cancelled():
        cancel_task(data, "Cancelled before the outputs filer or the end")

    # upload the outputs, if any
    if data["outputs"]:
        filerjob = new_job(
            filer.get_spec("outputs", args.debug),
            task_name + "-outputs-filer",
            args.namespace,
        )

        created_jobs.append(filerjob)

        # filerjob.run_to_completion(poll_interval)
        status = filerjob.run_to_completion(
            poll_interval, check_cancelled, args.pod_timeout
        )
        if status != "Complete":
            exit_cancelled("Got status " + status)
    # The claim of `init_pvc`, if any: deleted also when no outputs filer ran
    delete_created_pvc(keep_failed=False)


def newParser() -> argparse.ArgumentParser:
    """Create the parser of the command line.

    Returns:
        The parser.
    """
    parser = argparse.ArgumentParser(description="TaskMaster main module")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "json",
        help="string containing json TES request, required if -f is not given",
        nargs="?",
    )
    group.add_argument(
        "-f",
        "--file",
        help="TES request as a file or '-' for stdin, required if json is not given",
    )

    parser.add_argument(
        "-p",
        "--poll-interval",
        type=positive_seconds,
        help="Seconds between polls of a job",
        default=5,
    )
    parser.add_argument(
        "-pt",
        "--pod-timeout",
        type=int,
        help="Seconds after which a pending pod that can't start fails its job",
        default=240,
    )
    parser.add_argument(
        "-fn",
        "--filer-name",
        help="Filer image, without its tag",
        default="docker.io/elixircloud/tesk-core-filer",
    )
    parser.add_argument(
        "-fv", "--filer-version", help="Tag of the filer image", default="latest"
    )
    parser.add_argument(
        "-n", "--namespace", help="Kubernetes namespace to run in", default="default"
    )
    parser.add_argument(
        "-s",
        "--state-file",
        help="State file for state.py script",
        # Not used by the taskmaster; only a default path
        default="/tmp/.teskstate",  # noqa: S108 -- unused default, see above
    )
    parser.add_argument("-d", "--debug", help="Set debug mode", action="store_true")
    parser.add_argument(
        "--localKubeConfig",
        help="Read k8s configuration from localhost",
        action="store_true",
    )
    parser.add_argument(
        "--pull-policy-always",
        help="set imagePullPolicy = 'Always'",
        action="store_true",
    )

    return parser


def log_level(debug: bool) -> int:
    """Get the log level of the taskmaster.

    Args:
        debug: Whether `--debug` is set.

    Returns:
        `DEBUG` with `--debug`, otherwise `INFO`, so that the reasons why
        jobs fail or are deleted (e.g. a pod that can't start) and warnings
        about the settings show by default.
    """
    return logging.DEBUG if debug else logging.INFO


def newLogger(loglevel: int) -> logging.Logger:
    """Configure logging, with the Kubernetes client's messages left out.

    Args:
        loglevel: The log level.

    Returns:
        The logger of the taskmaster.
    """
    configure_logging(loglevel)
    logging.getLogger("kubernetes.client").setLevel(logging.CRITICAL)
    logger = logging.getLogger(__name__)

    return logger


def positive_seconds(value: str) -> float:
    """Parse the seconds between polls from the command line.

    Args:
        value: The argument.

    Returns:
        The seconds, from `min_poll_interval` to `max_poll_interval`.

    Raises:
        argparse.ArgumentTypeError: If the argument isn't a number in that
            range.
    """
    try:
        seconds = float(value)
    except ValueError:
        seconds = math.nan
    if not min_poll_interval <= seconds <= max_poll_interval:
        raise argparse.ArgumentTypeError(
            f"not a number from {min_poll_interval:g} to {max_poll_interval:g}: "
            f"{value!r}"
        )
    return seconds


def on_sigterm(signum: int, frame: FrameType | None) -> NoReturn:
    """Stop the taskmaster on `SIGTERM`, e.g. when Kubernetes stops its pod.

    The jobs are left as they are: a new attempt of the pod (e.g. after the
    node was drained) continues with them, and deleting the taskmaster's job
    deletes them with it (`ownerReferences`).

    Args:
        signum: The signal's number.
        frame: The current stack frame.
    """
    logger.error("Got SIGTERM; stopping, the jobs are kept")
    sys.exit(sigterm_exit_code)


def main() -> None:
    """Run the taskmaster on the command line.

    On `SIGINT` (e.g. Ctrl-C when run by hand), deletes the jobs that it
    created (`clean_on_interrupt`) and exits with code 130; on `SIGTERM`, see
    `on_sigterm`.
    """
    signal.signal(signal.SIGTERM, on_sigterm)
    try:
        run_main()
    except KeyboardInterrupt:
        clean_on_interrupt()
        sys.exit(sigint_exit_code)


def run_main() -> None:
    """Run the taskmaster with the arguments of the command line."""
    global args, logger, created_pvc, max_queue, poll_interval  # noqa: PLW0603 -- module state of the run

    have_json_pvc = False

    parser = newParser()

    args = parser.parse_args()

    poll_interval = args.poll_interval

    logger = newLogger(log_level(args.debug))
    logger.debug("Starting taskmaster")

    # Load kubernetes config file
    if args.localKubeConfig:
        config.load_kube_config()
    else:
        config.load_incluster_config()

    # Before the task is read, so that a taskmaster that fails on it gets its
    # TTL too, and before any job is created, also for a task cancelled now
    init_own_job()
    max_queue = max_queue_seconds()

    # Get input JSON
    if args.file is None:
        data = json.loads(args.json)
    elif args.file == "-":
        data = json.load(sys.stdin)
    elif args.file.endswith(".gz"):
        with gzip.open(args.file, "rb") as fh:
            data = json.loads(fh.read())
        have_json_pvc = True
    else:
        with open(args.file) as fh:
            data = json.load(fh)

    # Known before the claim is created, so that a task cancelled now also
    # deletes the claim that an earlier attempt of this pod left behind
    # after an unexpected error (a claim that doesn't exist is no error)
    created_pvc = task_pvc(data)

    # Check if we're cancelled during init
    if check_cancelled():
        cancel_task(data, "Cancelled during init")

    run_task(data, args.filer_name, args.filer_version, have_json_pvc)


def clean_on_interrupt() -> None:
    """Delete the jobs that the taskmaster created; log errors."""
    logger.error("Interrupted; deleting the jobs")

    for job in created_jobs:
        job.delete_quietly()


def task_job_names(data: dict[str, Any]) -> list[str]:
    """Name the jobs that the taskmaster runs for a task, in their order.

    Args:
        data: The task.

    Returns:
        The names of the inputs filer (if the task has inputs), of the
        executors, and of the outputs filer (if it has outputs); none for a
        task without executors, which is invalid.
    """
    executors = data.get("executors") or []
    if not executors:
        return []
    task_name = executors[0]["metadata"]["labels"]["taskmaster-name"]
    names = [task_name + "-inputs-filer"] if data.get("inputs") else []
    names.extend(executor["metadata"]["name"] for executor in executors)
    if data.get("outputs"):
        names.append(task_name + "-outputs-filer")
    return names


def is_job_finished(job: Any) -> bool:
    """Tell whether a job, as read from the Kubernetes API, has ended.

    Args:
        job: The job.

    Returns:
        Whether it has a condition that ends it (complete or failed).
    """
    conditions = (job.status.conditions if job.status else None) or []
    return any(
        condition.status == "True"
        and condition.type in JOB_COMPLETE_CONDITIONS | JOB_FAILED_CONDITIONS
        for condition in conditions
    )


def stop_task_jobs(data: dict[str, Any]) -> None:
    """Delete the task's jobs that are still running, by name.

    Covers jobs that an earlier attempt of the taskmaster's pod started,
    which this attempt doesn't know otherwise. Jobs that ended are kept, with
    the logs of their pods; a job that doesn't exist is skipped. A job that
    can't be read is deleted; errors are logged, not raised.

    Args:
        data: The task.
    """
    api = get_batch_api()
    for name in task_job_names(data):
        try:
            job = api.read_namespaced_job(
                name, args.namespace, _request_timeout=job_read_timeout
            )
        except ApiException as ex:
            if ex.status == 404:
                continue
            logger.error("Can't read job '%s': %s", name, error_reason(ex))
        except HTTPError as ex:
            logger.error("Can't read job '%s': %s", name, error_reason(ex))
        else:
            if is_job_finished(job):
                continue
        delete_job_quietly(api, name, args.namespace)


def cancel_task(data: dict[str, Any], reason: str) -> NoReturn:
    """Stop the task's running jobs (`stop_task_jobs`), then stop as cancelled.

    Args:
        data: The task.
        reason: Why the taskmaster stops.
    """
    stop_task_jobs(data)
    exit_cancelled(reason)


def keep_failed_pvc() -> bool:
    """Tell whether to keep the claim of a task that doesn't complete.

    `KEEP_FAILED_PVC` set to `true` (in any case) keeps it; unset, empty or
    `false` deletes it. Other values log a warning and delete it.

    Returns:
        Whether to keep the claim.
    """
    raw = os.environ.get(keep_failed_pvc_env, "")
    value = raw.strip().lower()
    if value not in ("", "true", "false"):
        logger.warning(
            "Ignoring %s=%r: not true or false; deleting the PVC",
            keep_failed_pvc_env,
            raw,
        )
    return value == "true"


def delete_created_pvc(*, keep_failed: bool = True) -> None:
    """Delete the task's claim, if the taskmaster created one, and forget it.

    An error is logged, not raised, so that the taskmaster goes on as it
    should; a claim that is already gone isn't an error.

    Args:
        keep_failed: Whether `keep_failed_pvc` can keep the claim: true when
            the taskmaster stops early, false when the task completed.
    """
    global created_pvc  # noqa: PLW0603 -- module state, see `clean_on_interrupt`
    pvc = created_pvc
    if pvc is None:
        return
    if keep_failed and keep_failed_pvc():
        logger.info("Keeping PVC '%s' (%s)", pvc.name, keep_failed_pvc_env)
        return
    # Forgotten first, so that it isn't deleted twice
    created_pvc = None
    try:
        pvc.delete(request_timeout=pvc_delete_timeout)
    except ApiException as ex:
        if ex.status != 404:
            logger.error("Can't delete PVC '%s': %s %s", pvc.name, ex.status, ex.reason)
    except HTTPError as ex:
        logger.error("Can't delete PVC '%s': %s", pvc.name, ex)


def exit_cancelled(reason: str = "Unknown reason") -> NoReturn:
    """Log why the taskmaster stops, delete the claim, and exit with code 0.

    The taskmaster stops this way when a job doesn't complete or the task is
    cancelled. The task won't use its claim again, so it is deleted
    (`delete_created_pvc`). Unexpected errors don't come here: they fail the
    taskmaster's pod, and a retry of the pod reuses the claim.

    Args:
        reason: Why the taskmaster stops.
    """
    logger.error("Cancelling taskmaster: %s", reason)
    delete_created_pvc()
    sys.exit(0)


def read_pod_labels() -> dict[str, str]:
    """Read the labels of the taskmaster's pod.

    The Downward API writes them as one `key="value"` line per label.

    Returns:
        The labels, or none if there is no labels file (e.g. outside a pod).
    """
    if not os.path.exists(pod_labels_file):
        return {}

    labels = {}
    with open(pod_labels_file) as fh:
        for line in fh.read().splitlines():
            key, sep, value = line.partition("=")
            if sep:
                labels[key] = value.strip('"')
    return labels


def log_job_read_error(job_name: str, ex: Exception) -> None:
    """Log that the taskmaster's job can't be read.

    The first error is logged at `ERROR`, so that it shows without
    `--debug`; later ones at `DEBUG`, so that a lasting error (e.g. a role
    without `get` on `jobs`) isn't logged at every poll.

    Args:
        job_name: The name of the taskmaster's job.
        ex: The error.
    """
    global job_read_error_logged  # noqa: PLW0603 -- once per run
    level = logging.DEBUG if job_read_error_logged else logging.ERROR
    job_read_error_logged = True
    logger.log(
        level,
        "Can't read job '%s' to check for cancellation: %s",
        job_name,
        error_reason(ex),
    )


def error_reason(ex: Exception) -> str:
    """Describe an error of the Kubernetes API, without its response body.

    Args:
        ex: The error.

    Returns:
        The status and reason of an API error, or the error's text.
    """
    return f"{ex.status} {ex.reason}" if isinstance(ex, ApiException) else str(ex)


def is_job_cancelled(job_name: str) -> bool:
    """Tell whether the TESK API labelled the taskmaster's job as cancelled.

    An error reading the job (also when it doesn't exist) is logged (see
    `log_job_read_error`), and
    counts as not cancelled: the job is read again at the next check. A
    missing job doesn't stop the taskmaster, so that a wrong namespace can't
    cancel every task.

    Args:
        job_name: The name of the taskmaster's job.

    Returns:
        Whether the job has the cancel label.
    """
    try:
        job = get_batch_api().read_namespaced_job(
            job_name, args.namespace, _request_timeout=job_read_timeout
        )
    except (ApiException, HTTPError) as ex:
        log_job_read_error(job_name, ex)
        return False
    job_labels = job.metadata.labels or {}
    cancelled: bool = job_labels.get(cancel_label) == cancel_value
    return cancelled


def check_cancelled() -> bool:
    """Tell whether the task was cancelled.

    The TESK API labels the taskmaster's job and, if it runs, its pod. The
    pod's labels are mounted as `pod_labels_file` (Downward API). A pod that
    was pending or not created yet when the task was cancelled has no label,
    so the job's label is read from the Kubernetes API as well.

    Returns:
        Whether the pod or the job has the cancel label.
    """
    labels = read_pod_labels()
    logger.debug("Got pod labels: %s", labels)
    if labels.get(cancel_label) == cancel_value:
        return True

    job_name = own_job_name(labels)
    if job_name is None:
        return False
    return is_job_cancelled(job_name)


def get_batch_api() -> client.BatchV1Api:
    """Get the client that reads and patches the taskmaster's job.

    Created at the first call, after the Kubernetes configuration is loaded,
    and reused by later calls.

    Returns:
        The client.
    """
    global batch_api  # noqa: PLW0603 -- one client for all polls
    if batch_api is None:
        batch_api = client.BatchV1Api()
    return batch_api


def own_job_name(labels: dict[str, str]) -> str | None:
    """Get the name of the taskmaster's job from the labels of its pod.

    Args:
        labels: The labels of the taskmaster's pod (`read_pod_labels`).

    Returns:
        The name, or none if no label has it (e.g. outside a pod).
    """
    return next((labels[key] for key in job_name_labels if key in labels), None)


def env_seconds(name: str, minimum: int, unset: str) -> int | None:
    """Get a number of seconds from a setting in the environment.

    Args:
        name: The environment variable.
        minimum: The smallest valid value; the largest is `max_seconds`.
        unset: What happens without a valid value, for the log.

    Returns:
        The seconds: a whole number from `minimum` to `max_seconds`. None if
        the setting is unset or empty, or invalid, which is logged.
    """
    return env_whole_number(name, minimum, unset, "a whole number of seconds")


def env_whole_number(name: str, minimum: int, unset: str, what: str) -> int | None:
    """Get a whole number from a setting in the environment.

    Args:
        name: The environment variable.
        minimum: The smallest valid value; the largest is `max_seconds`.
        unset: What happens without a valid value, for the log.
        what: What the value must be, for the log.

    Returns:
        The number, from `minimum` to `max_seconds`. None if the setting is
        unset or empty (also only spaces), or invalid, which is logged.
    """
    raw = os.environ.get(name, "")
    value = raw.strip()
    if not value:
        return None
    # Not more digits than the largest value: `int` limits the digits it
    # converts
    if (
        value.isascii()
        and value.isdigit()
        and len(value) <= len(str(max_seconds))
        and minimum <= int(value) <= max_seconds
    ):
        return int(value)
    logger.error(
        "Ignoring %s=%r: not %s from %d to %d; %s",
        name,
        raw,
        what,
        minimum,
        max_seconds,
        unset,
    )
    return None


def task_ttl_seconds() -> int | None:
    """Get the TTL of the taskmaster's job from `TASK_TTL_SECONDS`.

    Returns:
        The seconds after which Kubernetes deletes the finished job: a whole
        number from 0 to `max_seconds`. None if the setting is unset or
        empty, or invalid, which is logged.
    """
    return env_seconds(task_ttl_env, 0, "no TTL")


def max_queue_seconds() -> int | None:
    """Get the max queue time of the task's pods from `TESK_MAX_QUEUE_SECONDS`.

    Returns:
        The seconds after which a pod that can't be scheduled ends the task:
        a whole number from 1 to `max_seconds`. None if the setting is unset
        or empty, or invalid, which is logged: pods wait forever.
    """
    return env_seconds(max_queue_env, 1, "pods wait to be scheduled forever")


def init_own_job() -> None:
    """Make the taskmaster's job own the task's jobs, and set its TTL.

    The jobs that the taskmaster creates (`new_job`) get an owner reference
    to its job, with the job's name and UID from its pod's labels (no API
    call, so that every attempt of the pod sets it): Kubernetes deletes them,
    and their pods, with it. With `TASK_TTL_SECONDS`, sets the job's
    `ttlSecondsAfterFinished`, so that Kubernetes deletes it that many
    seconds after it finished; an error is logged, not raised, and the task
    runs without a TTL, as before.
    """
    global owner_references  # noqa: PLW0603 -- module state of the run
    labels = read_pod_labels()
    job_name = own_job_name(labels)
    job_uid = next((labels[key] for key in job_uid_labels if key in labels), None)
    if job_name is None or job_uid is None:
        logger.debug("No job name or UID in the pod's labels; jobs get no owner")
        return
    owner_references = [
        {"apiVersion": "batch/v1", "kind": "Job", "name": job_name, "uid": job_uid}
    ]

    ttl = task_ttl_seconds()
    if ttl is None:
        return
    try:
        get_batch_api().patch_namespaced_job(
            job_name,
            args.namespace,
            {"spec": {"ttlSecondsAfterFinished": ttl}},
            _request_timeout=job_read_timeout,
        )
    except (ApiException, HTTPError) as ex:
        logger.error("Can't set the TTL of job '%s': %s", job_name, error_reason(ex))


def new_job(
    body: dict[str, Any],
    name: str,
    namespace: str | None = None,
    *,
    delete_unschedulable: bool = False,
) -> Job:
    """Describe a job of the task, owned by the taskmaster's job.

    Sets the owner references of `init_own_job` as the manifest's only
    ones, so that no other owner keeps the job when the taskmaster's job is
    deleted (the TESK API sets none), and the max queue time (`max_queue`).

    Args:
        body: The manifest of the job, changed in place.
        name: Name of the job.
        namespace: Namespace of the job; that of the taskmaster if none.
        delete_unschedulable: Whether a job whose pod can't be scheduled in
            time is deleted (executors), rather than failed and kept
            (filers); see `Job.end_unschedulable`.

    Returns:
        The job, not created yet.
    """
    if owner_references:
        body["metadata"]["ownerReferences"] = list(owner_references)
    job = Job(body, name, args.namespace if namespace is None else namespace)
    job.max_queue = max_queue
    job.delete_unschedulable = delete_unschedulable
    return job


if __name__ == "__main__":
    main()
