import enum
import logging
import time
from kubernetes import client


@enum.unique
class State(enum.Enum):
    Initialized = 'Initialized'
    Running = 'Running'
    Cancelled = 'Cancelled'
    Complete = 'Complete'
    Failed = 'Failed'
    Error = 'Error'

class Job(object):
    _k8s_batch = client.BatchV1Api()

    def __init__(self, k8s_job, name='task-job', namespace='default'):
        self.name = name
        self.namespace = namespace
        self.status = State.Initialized
        self.k8s_job = k8s_job
        self.k8s_job['metadata']['name'] = name

    def run_to_completion(self, poll_interval, must_cancel):
        logging.debug(self.k8s_job)
        self._k8s_batch.create_namespaced_job(self.namespace, self.k8s_job)
        self.update_status_from_k8s()
        while self.status == State.Running:
            if must_cancel():
                self.delete()
                self.status = State.Cancelled
            else:
                time.sleep(poll_interval)
                self.update_status_from_k8s()

        return self.status

    def update_status_from_k8s(self):
        # needed to do a reverse search for the states from their string values
        valid_states = {state.value: state for state in [State.Complete, State.Failed]}

        job = self._k8s_batch.read_namespaced_job(self.name, self.namespace)
        try:
            job_condition = job.status.conditions[0]

            if job_condition.status and job_condition.type in valid_states.keys():
                self.status = valid_states[job_condition.type]
            else:
                self.status = State.Error
        except TypeError:  # The condition is not initialized, so the job is not complete yet
            self.status = State.Running

    def delete(self):
        self._k8s_batch.delete_namespaced_job(
            self.name, self.namespace, client.V1DeleteOptions())
