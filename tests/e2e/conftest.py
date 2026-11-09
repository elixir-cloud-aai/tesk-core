"""Fixtures of the end-to-end tests on Kubernetes.

The tests run tasks through the taskmaster's image in a cluster, as the TESK
API does: one taskmaster job per task, with the task's JSON and the labels
of its pod (Downward API). They need a cluster in the current kubeconfig
context with the images and manifests that `docs/development.md` describes
(CI: the job `e2e` of `.github/workflows/ci.yml`), and run only if
`TESK_E2E` is set (e.g. `TESK_E2E=1 pytest tests/e2e`).
"""

from collections.abc import Callable, Iterator
import os
from typing import Any

from kubernetes import client, config
import pytest

from tests.e2e.cluster import Task


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    """Skip the end-to-end tests unless `TESK_E2E` is set.

    Args:
        items: The collected tests.
    """
    if os.environ.get("TESK_E2E"):
        return
    skip = pytest.mark.skip(reason="end-to-end tests need TESK_E2E and a cluster")
    for item in items:
        if "tests/e2e/" in str(item.path).replace(os.sep, "/"):
            item.add_marker(skip)


@pytest.fixture(scope="session")
def batch() -> client.BatchV1Api:
    """Load the kubeconfig and get the client of the batch API.

    Returns:
        The client.
    """
    config.load_kube_config()
    return client.BatchV1Api()


@pytest.fixture(scope="session")
def core(batch: client.BatchV1Api) -> client.CoreV1Api:
    """Get the client of the core API (after `batch` loaded the kubeconfig).

    Args:
        batch: The client of the batch API.

    Returns:
        The client.
    """
    return client.CoreV1Api()


@pytest.fixture
def run_task(
    batch: client.BatchV1Api, core: client.CoreV1Api
) -> Iterator[Callable[..., Task]]:
    """Start tasks, and delete them afterwards.

    Args:
        batch: The client of the batch API.
        core: The client of the core API.

    Yields:
        A function that starts a task (see `Task`).
    """
    tasks: list[Task] = []

    def start(scripts: list[str], **kwargs: Any) -> Task:
        task = Task(batch, core, scripts, **kwargs)
        tasks.append(task)
        return task

    yield start
    for task in tasks:
        task.delete()
