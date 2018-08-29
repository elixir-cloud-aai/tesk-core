#!/usr/bin/env python2

import argparse
import json
import os
import sys
import logging
from kubernetes import config
from tesk_core.job import Job, State
from tesk_core.pvc import PVC
from tesk_core.filer_class import Filer

CREATED_JOBS = []
CREATED_PVC = None
POLL_INTERVAL = 5
DEBUG = False


def run_executor(executor, namespace, jobs, pvc=None):
    jobname = executor['metadata']['name']
    spec = executor['spec']['template']['spec']

    if pvc is not None:
        spec['containers'][0]['volumeMounts'] = pvc.volume_mounts
        spec['volumes'] = [pvc.volume()]

    logger.debug('Created job: %s', jobname)
    job = Job(executor, jobname, namespace)
    logger.debug('Job spec: %s', str(executor))

    jobs.append(job)

    status = job.run_to_completion(POLL_INTERVAL, is_task_cancelled)
    if status != State.Complete:
        exit_cancelled(pvc, 'Got status ' + status.value)


def dirname_from(iodata):
    if iodata['type'] == 'FILE':
        directory = os.path.dirname(iodata['path'])
        logger.debug('dirname of %s is: %s', iodata['path'], directory)
    elif iodata['type'] == 'DIRECTORY':
        directory = iodata['path']
    return directory


def init_pvc(task, task_name, volume_basename, namespace):
    pvc_name = task_name + '-pvc'
    pvc_size = task['resources']['disk_gb']

    # paths that need to be mounted
    paths = task['volumes']
    # inputs/outputs that need to be present, from FILE and DIRECTORY entries
    paths += [dirname_from(io) for io in task['inputs'] + task['outputs']]

    pvc = PVC(paths, volume_basename, pvc_name, pvc_size, namespace)

    logger.debug(pvc.volume_mounts)
    logger.debug(type(pvc.volume_mounts))
    return pvc

def download_inputs(filer, pvc, task_name, jobs, namespace):
    filer.set_volume_mounts(pvc.name, pvc.volume_mounts)
    filerjob = Job(
        filer.get_spec('inputs', DEBUG),
        task_name + '-inputs-filer',
        namespace)

    jobs.append(filerjob)

    status = filerjob.run_to_completion(POLL_INTERVAL, is_task_cancelled)
    if status != State.Complete:
        exit_cancelled(pvc, 'Got status ' + status.value)


def run_task(task, filer_version, namespace):
    global CREATED_PVC
    global CREATED_JOBS
    task_name = task['executors'][0]['metadata']['labels']['taskmaster-name']
    volume_basename = 'task-volume'
    pvc = None

    def io_in_job(params):
        return params['volumes'] or params['inputs'] or params['outputs']

    if io_in_job(task):
        filer = Filer(task_name + '-filer', task, filer_version, DEBUG)
        if os.environ.get('TESK_FTP_USERNAME') is not None:
            filer.set_ftp(
                os.environ['TESK_FTP_USERNAME'],
                os.environ['TESK_FTP_PASSWORD'])

        pvc = init_pvc(task, task_name, volume_basename, namespace)
        download_inputs(filer, pvc, task_name, CREATED_JOBS, namespace)

        # Store to a global to be able to clean up
        CREATED_PVC = pvc


    for executor in task['executors']:
        run_executor(executor, namespace, CREATED_JOBS, pvc)

    # run executors
    logger.debug("Finished running executors")

    # upload files and delete pvc
    if io_in_job(task):
        filerjob = Job(
            filer.get_spec('outputs', DEBUG),
            task_name + '-outputs-filer',
            namespace)

        CREATED_JOBS.append(filerjob)

        status = filerjob.run_to_completion(POLL_INTERVAL, is_task_cancelled)
        if status != State.Complete:
            exit_cancelled(pvc, 'Got status ' + status.value)
        else:
            pvc.delete()


def main():
    parser = argparse.ArgumentParser(description='TaskMaster main module')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        'json',
        help='string containing json TES request, required if -f is not given',
        nargs='?')
    group.add_argument(
        '-f',
        '--file',
        help='TES request as a file or \'-\' for stdin, required if json is not given')

    parser.add_argument(
        '-p',
        '--poll-interval',
        help='Job polling interval',
        default=5)
    parser.add_argument(
        '-fv',
        '--filer-version',
        help='Filer image version',
        default='v0.1.9')
    parser.add_argument(
        '-n',
        '--namespace',
        help='Kubernetes namespace to run in',
        default='default')
    parser.add_argument(
        '-s',
        '--state-file',
        help='State file for state.py script',
        default='/tmp/.teskstate')
    parser.add_argument(
        '-d',
        '--debug',
        help='Set debug mode',
        action='store_true')

    args = parser.parse_args()

    global DEBUG
    DEBUG = args.debug

    global POLL_INTERVAL
    POLL_INTERVAL = args.poll_interval

    loglevel = logging.DEBUG if DEBUG else logging.ERROR

    logging.basicConfig(
        format='%(asctime)s %(levelname)s: %(message)s',
        datefmt='%m/%d/%Y %I:%M:%S',
        level=loglevel)
    logging.getLogger('kubernetes.client').setLevel(logging.CRITICAL)

    global logger
    logger = logging.getLogger(__name__)
    logger.debug('Starting taskmaster')

    # Get input JSON
    if args.file is None:
        job_json = args.json
    elif args.file == '-':
        job_json = sys.stdin.read()
    else:
        with open(args.file) as input_file:
            job_json = input_file.read()

    task = json.loads(job_json)

    # Load kubernetes config file
    config.load_incluster_config()

    # Check if we're cancelled during init
    if is_task_cancelled():
        exit_cancelled(None, 'Cancelled during init')

    run_task(task, args.filer_version, args.namespace)


def clean_on_interrupt():
    logger.debug('Caught interrupt signal, deleting jobs and pvc')


    global CREATED_JOBS
    for job in CREATED_JOBS:
        job.delete()

    CREATED_JOBS = []

    global CREATED_PVC
    if CREATED_PVC is not None:
        CREATED_PVC.delete()

    CREATED_PVC = None


def exit_cancelled(pvc, reason='Unknown reason'):
    if pvc is not None:
        pvc.delete()
    logger.error('Cancelling taskmaster: %s', reason)
    sys.exit(0)


def is_task_cancelled():
    def is_cancelled(label):
        logging.debug('Got label: %s', label)
        _, value = label.split('=')
        return value == '"Cancelled"'

    with open('/podinfo/labels') as labels:
        return any(is_cancelled(label) for label in labels.readlines())


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        clean_on_interrupt()
