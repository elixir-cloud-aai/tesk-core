import pytest

from kubernetes.client import V1DeleteOptions
from tesk_core.job import Job


@pytest.fixture
def default_job(mocker):
    mocker.spy(Job, '_k8s_batch')
    body = {
        'metadata': {'name': 'default'},
    }
    return Job(body)

def test_attributes(default_job):
    assert default_job.name == 'task-job'
    assert default_job.namespace == 'default'
    assert default_job.k8s_job['metadata']['name'] == 'task-job'

def test_deletion(default_job):
    # check side-effects of deleting the job
    default_job.delete()
    default_job._k8s_batch.delete_namespaced_job.assert_called_once_with(
        default_job.name,
        default_job.namespace,
        V1DeleteOptions()
    )

def test_run(default_job):
    def never_cancel():
        return true

    default_job.run_to_completion(1, never_cancel)
