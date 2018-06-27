from tesk_core.job import Job


def test_job():
    job = Job({'metadata': {'name': 'test'}})
    assert job.name == 'task-job'
    assert job.namespace == 'default'
