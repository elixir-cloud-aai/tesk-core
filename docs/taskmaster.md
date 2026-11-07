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
exits with code 0. The TESK API derives the task's state from the jobs, so
the exit code doesn't report the failure. It also keeps the job of an
executor that `Failed`, so that the TESK API can report the state and the
executor's logs.

A pending pod whose containers can't start fails its job after
`--pod-timeout` seconds. That covers an image that can't be pulled
(`ErrImagePull`, `ImagePullBackOff`, `InvalidImageName`) and a broken
container configuration (`CreateContainerConfigError`, e.g. a missing secret
or ConfigMap). The timeout is measured from the pod's start, or from its
creation if it hasn't started; the taskmaster checks the pods only once the
job has been active for longer than `--pod-timeout`, and stops checking once
none of them is pending. It then deletes an executor's job (status
`Error`); a filer's job is kept. Pods that are pending for other reasons,
e.g. waiting to be scheduled or pulling a large image, are queued and keep
waiting.

To cancel a task, the TESK API labels the
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
| `EXECUTOR_BACKOFF_LIMIT` | Retries of each executor's job (`backoffLimit`); Kubernetes' default if unset. |
| `FILER_BACKOFF_LIMIT` | Retries of each filer job (`backoffLimit`); Kubernetes' default if unset. |
| `STORAGE_CLASS_NAME` | Storage class of the task's PVC; the cluster's default if unset. |
| `TESK_FTP_USERNAME`, `TESK_FTP_PASSWORD` | FTP credentials, passed on to the filer. |
| `NETRC_SECRET_NAME` | Name of a secret with a `.netrc` file (key `.netrc`), mounted into the filer for HTTP(S) and FTP credentials. |
| `HOST_BASE_PATH`, `CONTAINER_BASE_PATH`, `TRANSFER_PVC_NAME` | The shared volume of the `file:` protocol, passed on to the filer (see [Filer](filer.md#shared-volume-file)). |

The taskmaster also mounts the secret `aws-secret` into the filer, if it
exists, for S3 (see [Filer](filer.md#s3)).
