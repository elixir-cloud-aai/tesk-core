#!/usr/bin/env python3
"""The taskmaster: runs a TES task as Kubernetes jobs.

Creates the task's persistent volume claim, runs a filer job for the inputs,
one job per executor, one after another, and a filer job for the outputs.
"""

import argparse
import gzip
import json
import logging
import os
import re
import sys
from typing import Any, NoReturn

from kubernetes import config

from tesk_core.filer_class import Filer
from tesk_core.job import Job
from tesk_core.pvc import PVC

created_jobs: list[Job] = []
created_pvc: PVC | None = None
poll_interval = 5
task_volume_basename = "task-volume"
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
    if os.environ.get("EXECUTOR_BACKOFF_LIMIT") is not None:
        executor["spec"].update(
            {"backoffLimit": int(os.environ["EXECUTOR_BACKOFF_LIMIT"])}
        )
    if executor["spec"].get("backoffLimit") is None:
        executor["spec"]["backoffLimit"] = 0

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
    job = Job(executor, jobname, namespace)
    logger.debug("Job spec: %s", job.body)

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


def init_pvc(data: dict[str, Any], filer: Filer) -> PVC:
    """Create the task's persistent volume claim and stage the inputs.

    Exits (`exit_cancelled`) if the inputs filer job doesn't complete.

    Args:
        data: The task.
        filer: The filer job, which gets the claim mounted.

    Returns:
        The claim.
    """
    global created_pvc  # noqa: PLW0603 -- module state, see `clean_on_interrupt`

    task_name = data["executors"][0]["metadata"]["labels"]["taskmaster-name"]
    pvc_name = task_name + "-pvc"
    pvc_size = data["resources"]["disk_gb"]
    pvc = PVC(pvc_name, pvc_size, args.namespace)

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

    filerjob = Job(
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

        if os.environ.get("FILER_BACKOFF_LIMIT") is not None:
            filer.set_backoffLimit(int(os.environ["FILER_BACKOFF_LIMIT"]))

        pvc = init_pvc(data, filer)

    for executor in data["executors"]:
        run_executor(executor, args.namespace, pvc)

    # run executors
    logging.debug("Finished running executors")

    # upload files and delete pvc
    if data["volumes"] or data["inputs"] or data["outputs"]:
        filerjob = Job(
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
        elif pvc is not None:
            pvc.delete()


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

    parser.add_argument("-p", "--poll-interval", help="Job polling interval", default=5)
    parser.add_argument(
        "-pt",
        "--pod-timeout",
        type=int,
        help="Seconds after which a pod that can't pull its image fails its job",
        default=240,
    )
    parser.add_argument(
        "-fn",
        "--filer-name",
        help="Filer image version",
        default="eu.gcr.io/tes-wes/filer",
    )
    parser.add_argument(
        "-fv", "--filer-version", help="Filer image version", default="v0.1.9"
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


def newLogger(loglevel: int) -> logging.Logger:
    """Configure logging, with the Kubernetes client's messages left out.

    Args:
        loglevel: The log level.

    Returns:
        The logger of the taskmaster.
    """
    logging.basicConfig(
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%m/%d/%Y %I:%M:%S",
        level=loglevel,
    )
    logging.getLogger("kubernetes.client").setLevel(logging.CRITICAL)
    logger = logging.getLogger(__name__)

    return logger


def main() -> None:
    """Run the taskmaster on the command line."""
    global args, logger, created_pvc  # noqa: PLW0603 -- module state of the run

    have_json_pvc = False

    parser = newParser()

    args = parser.parse_args()

    # `--poll-interval` is not applied: the jobs are polled every 5 seconds
    # (`poll_interval` of the module)

    loglevel = logging.ERROR
    if args.debug:
        loglevel = logging.DEBUG

    logger = newLogger(loglevel)
    logger.debug("Starting taskmaster")

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

    # Load kubernetes config file
    if args.localKubeConfig:
        config.load_kube_config()
    else:
        config.load_incluster_config()

    created_pvc = None

    # Check if we're cancelled during init
    if check_cancelled():
        exit_cancelled("Cancelled during init")

    run_task(data, args.filer_name, args.filer_version, have_json_pvc)


def clean_on_interrupt() -> None:
    """Delete the jobs that the taskmaster created."""
    logger.debug("Caught interrupt signal, deleting jobs and pvc")

    for job in created_jobs:
        job.delete()


def exit_cancelled(reason: str = "Unknown reason") -> NoReturn:
    """Log why the taskmaster stops, and exit with code 0.

    Args:
        reason: Why the taskmaster stops.
    """
    logger.error("Cancelling taskmaster: %s", reason)
    sys.exit(0)


def check_cancelled() -> bool:
    """Tell whether the task was cancelled.

    The TESK API labels the taskmaster's pod; the labels are mounted as
    `/podinfo/labels` (Downward API).

    Returns:
        Whether a label has the value `"Cancelled"`.
    """
    labelInfoFile = "/podinfo/labels"

    if not os.path.exists(labelInfoFile):
        return False

    with open(labelInfoFile) as fh:
        for line in fh.readlines():
            _, label = line.split("=")
            logging.debug("Got label: %s", label)
            if label == '"Cancelled"':
                return True

    return False


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        clean_on_interrupt()
