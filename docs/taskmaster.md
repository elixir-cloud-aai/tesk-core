# Taskmaster

The taskmaster runs one TES task in Kubernetes. The TESK API starts one
taskmaster per task, as a Kubernetes job, and passes it the task as JSON: the
executors already converted to Kubernetes job specs, and the task's inputs,
outputs, volumes and resources.

## What it does

![Flow of the taskmaster, from main: the Kubernetes jobs that it creates and polls](images/taskmaster_architecture.png)

1. If the task has inputs, outputs or volumes, the taskmaster creates a
   persistent volume claim (PVC) of the task's disk size
   (`resources.disk_gb`) for them. It then runs the [filer](filer.md) as a
   job (`<task>-inputs-filer`) that downloads the inputs onto that volume.
2. It runs the executors, one after another, each as a job with the task's
   volume mounted. If an executor has resource requests but no limits, the
   requests also become its limits.
3. If the task has inputs, outputs or volumes, it runs the filer again
   (`<task>-outputs-filer`) to upload the outputs, then deletes the PVC.

The taskmaster polls each job until it completes. If a job doesn't complete,
the taskmaster stops without running the remaining jobs, keeps the PVC, and
exits with code 0. The TESK API derives the task's state from the jobs, not
from the taskmaster's exit code: an executor job that failed gives
`EXECUTOR_ERROR`, a filer job that failed `SYSTEM_ERROR`. So the taskmaster
keeps the jobs that failed, with the logs of their pods that ended.

A job counts as failed when Kubernetes sets its condition `Failed`, or
`FailureTarget`, which Kubernetes 1.31 and later set first; it counts as
complete with `Complete` or `SuccessCriteriaMet`. Other conditions, e.g.
`Suspended`, leave the job running.

A pending pod whose containers can't start fails its job after
`--pod-timeout` seconds. That covers an image that can't be pulled
(`ErrImagePull`, `ImagePullBackOff`, `InvalidImageName`) and a broken
container configuration (`CreateContainerConfigError`, e.g. a missing secret
or ConfigMap), for init containers too. The timeout is measured from the
pod's start, or from its creation if it hasn't started. The taskmaster checks
the pods only once the job has been active for longer than `--pod-timeout`,
then at most once a minute, and stops checking once none of them is pending.
When a pod of the job fails, it checks again, so that a retry or the
replacement of an evicted pod that can't start times out too. A
pull error that comes after the pod has waited longer than `--pod-timeout`,
e.g. after a long pull of a large image, fails the job at the next check.

To fail the job, the taskmaster sets its
`activeDeadlineSeconds` to 1 second, so that Kubernetes deletes the job's
active pods and marks the job `Failed` with the reason `DeadlineExceeded`
(the pod's events show why it couldn't start). The taskmaster then handles it
like any job that failed, also after a restart of the taskmaster. If
Kubernetes hasn't marked the job within another `--pod-timeout`, the
taskmaster stops anyway and keeps the job. If the Kubernetes API rejects the
change (e.g., the taskmaster's role lacks `patch` on jobs), the taskmaster
deletes the job, as earlier releases did, and stops; after other errors, it
tries again at the next poll. Pods that are pending for other reasons, e.g.
waiting to be scheduled or pulling a large image, are queued and keep
waiting.

Executor jobs aren't retried (`backoffLimit: 0`) unless
`EXECUTOR_BACKOFF_LIMIT` (or the job) sets a limit, so that the state and logs
are those of the executor's only run. The limit also covers pods that
Kubernetes evicts, e.g. from a drained node: without retries, an evicted
executor fails. TESK's Helm chart sets `EXECUTOR_BACKOFF_LIMIT` from
`tesk.executorRetries` (2 by default); set it to 0 for single runs. The TESK
API creates executor jobs with the restart policy `Never`; with `OnFailure`,
Kubernetes would also restart failed containers within the pod.

To cancel a task, the TESK API labels the taskmaster's pod: the taskmaster
reads its labels from `/podinfo/labels` (a Kubernetes downward API volume)
while it polls, and when it finds the value `Cancelled`, it deletes the
running job, also one that it is failing, and stops. If the taskmaster is
interrupted (`SIGINT`), it deletes the jobs it created.

## Options

The TESK API sets these in the taskmaster's job; they are listed for
development and debugging.

| Option | Default | Description |
|---|---|---|
| `JSON` (positional) or `-f`, `--file FILE` | | The task: as a JSON string, or as a file (gzipped if it ends with `.gz`; `-` for stdin). The TESK API passes `/jsoninput/JSON_INPUT.gz`. |
| `-n`, `--namespace` | `default` | Kubernetes namespace of the jobs and the PVC. |
| `-fn`, `--filer-name` | `eu.gcr.io/tes-wes/filer` | Image of the filer, without its tag. |
| `-fv`, `--filer-version` | `v0.1.9` | Tag of the filer image. |
| `-p`, `--poll-interval` | `5` | Seconds between polls of a job. |
| `-pt`, `--pod-timeout` | `240` | Seconds after which a pending pod that can't start fails its job (see above). |
| `-d`, `--debug` | off | Debug logging, also of the filer. |
| `--pull-policy-always` | off | Pull the filer image on every start (`imagePullPolicy: Always`). |
| `--localKubeConfig` | off | Use the local kubeconfig instead of the in-cluster configuration (for running the taskmaster outside a cluster). |

The defaults of `--filer-name` and `--filer-version` are outdated; the TESK
API always passes the filer image of its configuration.

## Environment variables

The TESK API passes each property `tesk.api.taskmaster.environment.<name>` of
its configuration to the taskmaster as the environment variable `<NAME>`
(upper case, dots as underscores); TESK's Helm chart sets them. The FTP
credentials come from a Kubernetes secret (see [Filer](filer.md#ftp)).

| Variable | Description |
|---|---|
| `EXECUTOR_BACKOFF_LIMIT` | Retries of each executor's job (`backoffLimit`); 0 if unset, unless the TESK API sets a limit in the job. |
| `FILER_BACKOFF_LIMIT` | Retries of each filer job (`backoffLimit`); Kubernetes' default if unset. |
| `STORAGE_CLASS_NAME` | Storage class of the task's PVC; the cluster's default if unset. |
| `TESK_FTP_USERNAME`, `TESK_FTP_PASSWORD` | FTP credentials, passed on to the filer. |
| `TESK_FTP_HOSTS` | Hosts that the filer sends the FTP credentials to, passed on to the filer with them (see [Filer](filer.md#ftp)). |
| `NETRC_SECRET_NAME` | Name of a secret with a `.netrc` file (key `.netrc`), mounted into the filer for HTTP(S) and FTP credentials. |
| `HOST_BASE_PATH`, `CONTAINER_BASE_PATH`, `TRANSFER_PVC_NAME` | The shared volume of the `file:` protocol, passed on to the filer (see [Filer](filer.md#shared-volume-file)). |

The taskmaster also mounts the secret `aws-secret` into the filer, if it
exists, for S3 (see [Filer](filer.md#s3)).
