"""End-to-end tests: tasks run by the taskmaster on Kubernetes.

See `conftest.py` for what they need. Each test checks the jobs, the PVC
and, where there are outputs, what the HTTP server received: what the TESK
API derives a task's state from.
"""

from collections.abc import Callable
import json
import uuid

from kubernetes import client

from tests.e2e.cluster import (
    HTTP_POD,
    HTTP_URL,
    NAMESPACE,
    QUOTA_NAMESPACE,
    Task,
    condition,
)


def test_task_with_inputs_and_outputs(
    run_task: Callable[..., Task], core: client.CoreV1Api, batch: client.BatchV1Api
) -> None:
    """Test that a task stages its inputs, runs, uploads, and deletes its PVC.

    One input has content, one comes from the HTTP server; the executor
    concatenates them into the output, which the filer uploads to the HTTP
    server. The jobs of the task, the filers' too, have the label
    `taskmaster-name`, by which the TESK API selects them.
    """
    upload = f"/out-{uuid.uuid4().hex[:8]}.txt"
    task = run_task(
        ["cat /data/in/a.txt /data/in/b.txt > /data/out/c.txt"],
        inputs=[
            {"path": "/data/in/a.txt", "type": "FILE", "content": "content-a;"},
            {"path": "/data/in/b.txt", "type": "FILE", "url": f"{HTTP_URL}/b.txt"},
        ],
        outputs=[{"path": "/data/out/c.txt", "type": "FILE", "url": HTTP_URL + upload}],
    )

    assert task.wait_finished() == "Complete", task.logs()

    for suffix in ("-inputs-filer", "-ex-00", "-outputs-filer"):
        job = task.job(suffix)
        assert job is not None, suffix
        assert condition(job, "Complete"), suffix
    # The taskmaster's job isn't among them: the TESK API (and `Task`) give
    # it only `job-type`
    labelled = batch.list_namespaced_job(
        NAMESPACE, label_selector=f"taskmaster-name={task.name}"
    )
    assert sorted(job.metadata.name for job in labelled.items) == [
        task.name + "-ex-00",
        task.name + "-inputs-filer",
        task.name + "-outputs-filer",
    ]
    log = core.read_namespaced_pod_log(HTTP_POD, NAMESPACE)
    assert f"PUT {upload} 'content-a;input /b.txt'" in log
    # The output file logs (TES `logs[].outputs`) for the TESK API
    filer = task.job("-outputs-filer")
    assert json.loads(filer.metadata.annotations["output-file-logs"]) == [
        {"url": HTTP_URL + upload, "path": "/data/out/c.txt", "size_bytes": "22"}
    ]
    task.wait(task.pvc_gone, "the PVC to be deleted")


def test_failing_executor(run_task: Callable[..., Task]) -> None:
    """Test that a failed executor is kept failed, and no later job runs.

    The taskmaster's own job completes: the TESK API derives the task's
    state (`EXECUTOR_ERROR`) from the executor's failed job.
    """
    task = run_task(["exit 3", "true"])

    assert task.wait_finished() == "Complete", task.logs()

    assert condition(task.job("-ex-00"), "Failed")
    assert task.job("-ex-01") is None


def test_executor_ignoring_error(
    run_task: Callable[..., Task], core: client.CoreV1Api
) -> None:
    """Test that the task goes on after an executor whose error is ignored.

    The failed executor's job is kept failed, so that the TESK API reports
    its exit code; the next executor and the outputs filer run, and the PVC
    is deleted as for a task that completed.
    """
    upload = f"/out-{uuid.uuid4().hex[:8]}.txt"
    task = run_task(
        ["exit 3", "echo -n after > /data/out/c.txt"],
        outputs=[{"path": "/data/out/c.txt", "type": "FILE", "url": HTTP_URL + upload}],
        ignore_error={0},
    )

    assert task.wait_finished() == "Complete", task.logs()

    assert condition(task.job("-ex-00"), "Failed")
    for suffix in ("-ex-01", "-outputs-filer"):
        job = task.job(suffix)
        assert job is not None, suffix
        assert condition(job, "Complete"), suffix
    assert "ignoring the error" in task.logs()
    log = core.read_namespaced_pod_log(HTTP_POD, NAMESPACE)
    assert f"PUT {upload} 'after'" in log
    task.wait(task.pvc_gone, "the PVC to be deleted")


def test_cancel_running_task(run_task: Callable[..., Task]) -> None:
    """Test that a task cancelled while an executor runs deletes that job.

    The taskmaster's own job completes; the TESK API reports the task as
    cancelled from the label.
    """
    task = run_task(["sleep 600"])
    task.wait(lambda: task.running("-ex-00"), "the executor to run")

    task.batch.patch_namespaced_job(
        task.name, NAMESPACE, {"metadata": {"labels": {"task-status": "Cancelled"}}}
    )

    assert task.wait_finished() == "Complete", task.logs()
    task.wait(lambda: task.gone("-ex-00"), "the executor job to be deleted")


def test_cancel_before_start(run_task: Callable[..., Task]) -> None:
    """Test that a task cancelled before its taskmaster's pod existed never runs.

    Only the taskmaster's job has the cancel label, not its pod: the
    taskmaster reads it from its job and creates no job; its own job
    completes (elixir-cloud-aai/tesk-core#132).
    """
    task = run_task(["sleep 600"], job_labels={"task-status": "Cancelled"})

    assert task.wait_finished() == "Complete", task.logs()
    assert task.job("-ex-00") is None
    assert "Cancelled during init" in task.logs()


def test_cancel_on_deadline_stops_jobs(run_task: Callable[..., Task]) -> None:
    """Test that a task cancelled as the TESK API does it leaves no jobs running.

    The TESK API labels the taskmaster's job (its pod may not have the label
    yet) and sets `activeDeadlineSeconds: 1`, so that Kubernetes fails the
    job and stops its pod (`SIGTERM`). The taskmaster polls only once an hour
    here, so only `SIGTERM` makes it stop the running executor's job
    (elixir-cloud-aai/tesk-core#132).
    """
    task = run_task(["sleep 600"], args=["-p", "3600"])
    task.wait(lambda: task.running("-ex-00"), "the executor to run")

    task.batch.patch_namespaced_job(
        task.name,
        NAMESPACE,
        {
            "metadata": {"labels": {"task-status": "Cancelled"}},
            "spec": {"activeDeadlineSeconds": 1},
        },
    )

    assert task.wait_finished() == "Failed", task.logs()
    task.wait(lambda: task.gone("-ex-00"), "the executor job to be deleted")


def test_unschedulable_task_times_out(run_task: Callable[..., Task]) -> None:
    """Test that an executor that can't be scheduled ends the task.

    With `TESK_MAX_QUEUE_SECONDS`, the taskmaster logs why and deletes the
    executor's job, from which the TESK API derives `SYSTEM_ERROR` (a
    missing executor); the taskmaster's own job completes.
    """
    task = run_task(
        ["true"],
        env={"TESK_MAX_QUEUE_SECONDS": "10"},
        resources={"requests": {"cpu": "1000"}},
    )

    assert task.wait_finished() == "Complete", task.logs()
    task.wait(lambda: task.gone("-ex-00"), "the executor job to be deleted")
    assert "wasn't scheduled within 10 s" in task.logs()


def test_task_in_namespace_that_requires_limits(
    run_task: Callable[..., Task], core: client.CoreV1Api
) -> None:
    """Test that a task runs where every container needs resource limits.

    In a namespace with a `ResourceQuota` on `limits.cpu` and a default
    memory limit below the executor's request (`tests/e2e/manifests/
    quota/`): the filers get their default limits, and the executor, with
    CPU and memory requests but only a CPU limit (as the TESK API sets
    `cpu_cores` and `ram_gb`), gets its memory limit from its request. The
    executor's memory request (600Mi) must stay above the namespace's
    default limit (512Mi in `quota.yaml`), which made its pod invalid before.
    """
    upload = f"/out-{uuid.uuid4().hex[:8]}.txt"
    task = run_task(
        ["cat /data/in/a.txt > /data/out/b.txt"],
        inputs=[{"path": "/data/in/a.txt", "type": "FILE", "content": "quota;"}],
        outputs=[{"path": "/data/out/b.txt", "type": "FILE", "url": HTTP_URL + upload}],
        resources={
            "requests": {"cpu": "100m", "memory": "600Mi"},
            "limits": {"cpu": "100m"},
        },
        namespace=QUOTA_NAMESPACE,
        taskmaster_resources={
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "1", "memory": "512Mi"},
        },
    )

    assert task.wait_finished() == "Complete", task.logs()

    for suffix in ("-inputs-filer", "-ex-00", "-outputs-filer"):
        job = task.job(suffix)
        assert job is not None, suffix
        assert condition(job, "Complete"), suffix
    executor = task.job("-ex-00").spec.template.spec.containers[0]
    assert executor.resources.limits == {"cpu": "100m", "memory": "600Mi"}
    for suffix in ("-inputs-filer", "-outputs-filer"):
        filer = task.job(suffix).spec.template.spec.containers[0]
        assert filer.resources.limits == {"cpu": "1", "memory": "512Mi"}, suffix
    log = core.read_namespaced_pod_log(HTTP_POD, NAMESPACE)
    assert f"PUT {upload} 'quota;'" in log


def test_executor_that_cant_be_created_ends_task(
    run_task: Callable[..., Task],
) -> None:
    """Test that an executor whose pod Kubernetes refuses ends the task.

    Its memory (2Gi, a limit from its request) is above the maximum of the
    namespace's `LimitRange` (1Gi in `quota.yaml`), so the job never gets a
    pod (`FailedCreate`). With the right to list events, which only this
    namespace grants, the taskmaster logs Kubernetes' message and deletes
    the executor's job without waiting for a max queue time, from which the
    TESK API derives `SYSTEM_ERROR`; before, the task stayed `RUNNING`
    (issue #169).
    """
    task = run_task(
        ["true"],
        resources={
            "requests": {"cpu": "100m", "memory": "2Gi"},
            "limits": {"cpu": "100m"},
        },
        namespace=QUOTA_NAMESPACE,
        taskmaster_resources={
            "requests": {"cpu": "100m", "memory": "128Mi"},
            "limits": {"cpu": "1", "memory": "512Mi"},
        },
    )

    assert task.wait_finished() == "Complete", task.logs()
    task.wait(lambda: task.gone("-ex-00"), "the executor job to be deleted")
    logs = task.logs()
    assert "can't be created" in logs, logs
    assert "maximum memory usage per Container is 1Gi" in logs, logs
