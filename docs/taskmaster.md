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

The taskmaster polls each job until it completes. If a job fails, or a pod
of it can't pull its image for longer than `--pod-timeout`, the taskmaster
stops, and the task fails; it deletes an executor's job that ended in an
error, but not the PVC. To cancel a task, the TESK API labels the
taskmaster's pod: the taskmaster reads its labels from `/podinfo/labels` (a
Kubernetes downward API volume) while it polls, and when it finds the value
`Cancelled`, it deletes the running job and stops.

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
| `-pt`, `--pod-timeout` | `240` | Seconds that a job's pod may take to start. |
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
| `EXECUTOR_BACKOFF_LIMIT` | Retries of each executor's job (`backoffLimit`); Kubernetes' default if unset. |
| `FILER_BACKOFF_LIMIT` | Retries of each filer job (`backoffLimit`); Kubernetes' default if unset. |
| `STORAGE_CLASS_NAME` | Storage class of the task's PVC; the cluster's default if unset. |
| `TESK_FTP_USERNAME`, `TESK_FTP_PASSWORD` | FTP credentials, passed on to the filer. |
| `NETRC_SECRET_NAME` | Name of a secret with a `.netrc` file (key `.netrc`), mounted into the filer for HTTP(S) and FTP credentials. |
| `HOST_BASE_PATH`, `CONTAINER_BASE_PATH`, `TRANSFER_PVC_NAME` | The shared volume of the `file:` protocol, passed on to the filer (see [Filer](filer.md#shared-volume-file)). |

The taskmaster also mounts the secret `aws-secret` into the filer, if it
exists, for S3 (see [Filer](filer.md#s3)).
