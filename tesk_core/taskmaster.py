#!/usr/bin/env python2

import argparse
import json
import os
import sys
import logging
from kubernetes import config
from tesk_core.job import Job
from tesk_core.pvc import PVC
from tesk_core.filer_class import Filer

CREATED_JOBS = []
CREATED_PVC = None
POLL_INTERVAL = 5


def run_executor(executor, namespace, jobs, pvc=None):
    jobname = executor['metadata']['name']
    spec = executor['spec']['template']['spec']

    if pvc is not None:
        spec['containers'][0]['volumeMounts'] = pvc.volume_mounts
        spec['volumes'] = [pvc.volume()]

    logger.debug('Created job: %s', jobname)
    job = Job(executor, jobname, namespace)
    logger.debug('Job spec: %s', str(job.body))

    jobs.append(job)

    status = job.run_to_completion(POLL_INTERVAL, check_cancelled)
    if status != 'Complete':
        exit_cancelled(pvc, 'Got status ' + status)


def dirname_from(iodata):
    if iodata['type'] == 'FILE':
        directory = os.path.dirname(iodata['path'])
        logger.debug('dirname of %s is: %s', iodata['path'], directory)
    elif iodata['type'] == 'DIRECTORY':
        directory = iodata['path']
    return directory


def init_pvc(data, task_name, volume_basename):
    pvc_name = task_name + '-pvc'
    pvc_size = data['resources']['disk_gb']

    # paths that need to be mounted
    paths = data['volumes']
    # inputs/outputs that need to be present, from FILE and DIRECTORY entries
    paths += [dirname_from(io) for io in data['inputs'] + data['outputs']]

    pvc = PVC(paths, volume_basename, pvc_name, pvc_size, args.namespace)

    logger.debug(pvc.volume_mounts)
    logger.debug(type(pvc.volume_mounts))
    return pvc

def download_inputs(filer, pvc, task_name, jobs):
    filer.set_volume_mounts(pvc.name, pvc.volume_mounts)
    filerjob = Job(
        filer.get_spec('inputs', args.debug),
        task_name + '-inputs-filer',
        args.namespace)

    jobs.append(filerjob)

    status = filerjob.run_to_completion(POLL_INTERVAL, check_cancelled)
    if status != 'Complete':
        exit_cancelled(pvc, 'Got status ' + status)


def run_task(data, filer_version):
    global CREATED_PVC
    global CREATED_JOBS
    task_name = data['executors'][0]['metadata']['labels']['taskmaster-name']
    volume_basename = 'task-volume'
    pvc = None

    if data['volumes'] or data['inputs'] or data['outputs']:

        filer = Filer(task_name + '-filer', data, filer_version, args.debug)
        if os.environ.get('TESK_FTP_USERNAME') is not None:
            filer.set_ftp(
                os.environ['TESK_FTP_USERNAME'],
                os.environ['TESK_FTP_PASSWORD'])

        pvc = init_pvc(data, task_name, volume_basename)
        download_inputs(filer, pvc, task_name, CREATED_JOBS)

        # Store to a global to be able to clean up
        CREATED_PVC = pvc


    for executor in data['executors']:
        run_executor(executor, args.namespace, CREATED_JOBS, pvc)

    # run executors
    logger.debug("Finished running executors")

    # upload files and delete pvc
    if data['volumes'] or data['inputs'] or data['outputs']:
        filerjob = Job(
            filer.get_spec('outputs', args.debug),
            task_name + '-outputs-filer',
            args.namespace)

        CREATED_JOBS.append(filerjob)

        status = filerjob.run_to_completion(POLL_INTERVAL, check_cancelled)
        if status != 'Complete':
            exit_cancelled(pvc, 'Got status ' + status)
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

    global args
    args = parser.parse_args()

    global POLL_INTERVAL
    POLL_INTERVAL = args.poll_interval

    loglevel = logging.DEBUG if args.debug else logging.ERROR

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
        data = json.loads(args.json)
    elif args.file == '-':
        data = json.load(sys.stdin)
    else:
        with open(args.file) as input_file:
            data = json.load(input_file)

    # Load kubernetes config file
    config.load_incluster_config()

    # Check if we're cancelled during init
    if check_cancelled():
        exit_cancelled(None, 'Cancelled during init')

    run_task(data, args.filer_version)


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


def check_cancelled():
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
