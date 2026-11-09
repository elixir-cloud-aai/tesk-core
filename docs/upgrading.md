# Upgrading

Changes that operators must act on when they upgrade the taskmaster and the
filer. Upgrade both images together: the taskmaster passes the settings on
to the filer.

## Unreleased

### Tasks that can't be scheduled can time out

A task whose pod the scheduler can't place, e.g. because it asks for more
CPU or memory than any node has, waited forever as `QUEUED`. The new
setting `TESK_MAX_QUEUE_SECONDS` ends such a task after that many seconds
with `SYSTEM_ERROR`, and logs why (see
[Taskmaster](taskmaster.md#what-it-does)). It is unset by default: tasks
wait as before. An executor whose pod can't be scheduled in time has its
job deleted; the TESK API reports that as `SYSTEM_ERROR` from the release
that derives the task state from all jobs of a task
([tesk-api#104](https://github.com/elixir-cloud-aai/tesk-api/issues/104)).
TESK API 1.1 and earlier report a task whose later executor was deleted as
`COMPLETE`: set `TESK_MAX_QUEUE_SECONDS` only with a newer TESK API.

### Finished tasks can be deleted after a TTL

The taskmaster's job now owns the task's filer and executor jobs
(`ownerReferences`), so deleting it deletes them and their pods too. The
new setting `TASK_TTL_SECONDS` lets Kubernetes delete the taskmaster's job
that many seconds after it finished (see
[Taskmaster](taskmaster.md#deleting-finished-tasks)). It is unset by
default: nothing is deleted, as before. Deleted tasks disappear from the
TESK API (`ListTasks`, `GetTask`), as TESK keeps no database.

The taskmaster takes its job's name and UID from its pod's labels, and sets
the TTL with `patch` on `jobs`, which the role of TESK's Helm chart already
grants (`charts/tesk/templates/common/taskmaster-rbac.yaml`); deployments
with their own role must grant it too. Without it, the taskmaster logs an
error and runs the task without a TTL. Jobs of tasks that started before
the upgrade have no owner, and stay. The task's ConfigMap, and PVCs that
are kept, aren't deleted with the jobs; pick a TTL of hours rather than
seconds (see the linked section).

### One FTP connection per uploaded directory

The filer uploads a directory over a single FTP connection; before, it
connected and logged in again for each file and subdirectory, which FTP
servers that limit connections or logins could refuse. Nothing to change.

### Streamed HTTP downloads, S3 directory inputs, merged `file:` copies

Nothing to change:

- The filer streams HTTP(S) inputs to disk instead of holding them in
  memory whole, so the filer's memory no longer limits the size of inputs,
  and reads only the start of error responses for its log (see
  [HTTP(S)](filer.md#https)).
- S3 directory inputs skip, with a warning, objects whose keys would place
  them outside the input's directory (e.g. with `..` segments), and create
  the directories of folder markers (keys that end with `/`). They also
  download objects beyond the first 1000 of the listing, which were
  missing before (see [S3](filer.md#s3)).
- `file:` directories copied into existing ones now merge subdirectories
  that exist on both sides at any depth, instead of failing; directories
  that exist keep their mode, and links in them are replaced (see
  [Shared volume](filer.md#shared-volume-file)).

### Filers run only when they have something to stage

The taskmaster runs the inputs filer (`<task>-inputs-filer`) only for tasks
with inputs, and the outputs filer (`<task>-outputs-filer`) only for tasks
with outputs; before, both ran for every task with inputs, outputs or
volumes. Tasks have fewer jobs and pods, and start and end sooner. Nothing
to change, unless your own tools expect both filer jobs of a task.

### PVCs of failed and cancelled tasks are deleted

The taskmaster used to delete a task's PVC only after the outputs were
uploaded: tasks that failed or were cancelled left their PVC behind. Now
the taskmaster deletes the PVC whenever it stops because a job didn't
complete or the task was cancelled (see
[Taskmaster](taskmaster.md#what-it-does)). As for completed tasks,
Kubernetes frees the storage once the task's jobs are deleted. PVCs left
behind by earlier releases stay: list them with
`kubectl get pvc --namespace <namespace>`, and delete those of tasks that
have ended (`<task>-pvc`, where `<task>` is the task's ID).

To keep the earlier behaviour, e.g. to inspect the files of failed tasks,
set `KEEP_FAILED_PVC` to `true` in the taskmaster's environment: the TESK
API passes it on from its own variable
`TESK_API_TASKMASTER_ENVIRONMENT_KEEP_FAILED_PVC` (see
[Environment variables](taskmaster.md#environment-variables)), which TESK's
Helm chart will set from a value (elixir-cloud-aai/TESK#261). A PVC that the taskmaster can't delete (e.g. a role
without `delete` on `persistentvolumeclaims`) is now logged as an error,
also for a completed task, which no longer fails the taskmaster's pod.

### Early cancellation stops the task

A task cancelled while its taskmaster's pod was pending, or before the pod
existed, used to run to its end, although the TESK API reported it as
`CANCELED`: the TESK API labels only a running pod. The taskmaster now also
reads the cancel label of its own job from the Kubernetes API, and stops
before it creates any job. This needs `get` on `jobs` for the taskmaster's
service account, which the role of TESK's Helm chart grants
(`charts/tesk/templates/common/taskmaster-rbac.yaml`); deployments with their own
role must grant it too. Without it, the taskmaster logs an error ("Can't read
job ... to check for cancellation") and stops only on the pod's label, as
before. Only the label
`task-status: Cancelled` cancels a task now, not any label with the value
`Cancelled`; the TESK API sets this one.

### FTP credentials go only to configured hosts

The filer sends the FTP credentials of `TESK_FTP_USERNAME` and
`TESK_FTP_PASSWORD` only to the hosts in the new setting `TESK_FTP_HOSTS`
(see [Filer](filer.md#ftp)). Without it, it logs in to every FTP server
anonymously, and logs an error that says so.

To keep using these credentials, do one of the following before you
upgrade:

- Set `TESK_FTP_HOSTS` in the taskmaster's environment to the hosts of the
  FTP account, e.g. `ftp.example.org`. With TESK's Helm chart, set
  `ftp.hosts` (see
  [Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in
  the TESK documentation).
- Or put the credentials into a `.netrc` file, with one entry per host (see
  [Credentials from `.netrc`](filer.md#credentials-from-netrc)).

Deployments without these credentials, or with a `.netrc` file, need no
change.

### HTTP(S) timeout

The filer's HTTP(S) requests now time out after 300 seconds without
progress (connecting, or waiting for the next part of a response or
upload); before, they could wait forever. Large files still work: the
timeout doesn't limit the whole transfer. For uploads, it also covers the
time a server takes to answer after the last byte (e.g., to check a large
file): raise it for such servers. To change it, set
`FILER_HTTP_TIMEOUT` (seconds) in the taskmaster's environment, e.g. with
TESK's Helm chart. Slow servers that take longer than that to start a
response now fail the transfer.

Also, HTTP error bodies in the filer's log are cut after 1000 characters,
and job manifests in the taskmaster's debug log show URLs without queries
and user info, and no FTP credentials.

### FTP URLs with a port

The filer now connects to FTP URLs with a port (e.g.
`ftp://ftp.example.org:2121/data`); before, these failed. Credentials go to
such a URL (with a port other than 21) only from an entry with that port:
`machine ftp.example.org:2121` in `.netrc`, or `ftp.example.org:2121` in
`TESK_FTP_HOSTS`. A `.netrc` entry without a port (`machine
ftp.example.org`), as in existing files, and an entry of `TESK_FTP_HOSTS`
without a port are used for port 21 only; the `default` entry of `.netrc` is
used for every host and port. URLs without a port work as before. If your
tasks use FTP URLs with other ports, add entries with those ports.

### `.netrc` and the env credentials together

With a `.netrc` file, the filer used to ignore `TESK_FTP_USERNAME` and
`TESK_FTP_PASSWORD`, and logged in anonymously to hosts without an entry.
Now it uses them for hosts without an entry that are in `TESK_FTP_HOSTS`
(see [Filer](filer.md#ftp)). Entries of the `.netrc` file still win: one
for the host and port or for the host, and also a `default` entry, which
applies to every host. No change is needed, unless you listed hosts in
`TESK_FTP_HOSTS` that must still be accessed anonymously.

If the env credentials are set as well, but `TESK_FTP_HOSTS` is not, the
filer now logs an error about `TESK_FTP_HOSTS` when it logs in to a host
without an entry. Set `TESK_FTP_HOSTS`, or remove the env credentials if
the `.netrc` file has all you need. An empty `TESK_FTP_USERNAME` counts as
no credentials, with or without `.netrc`.

### Pods that can't start time out

Pending pods now time out (`--pod-timeout`) for more reasons than
`ImagePullBackOff`: also `ErrImagePull`, `InvalidImageName` and
`CreateContainerConfigError`, for init containers too, and also before the
pod has started (measured from its creation). Their job fails and is kept,
as described below. Pods that are pending for other reasons, e.g. waiting to
be scheduled, still wait; the taskmaster checks their pods once a minute.
No change is needed.

### Failed jobs are kept

- The taskmaster no longer deletes a job that
  failed. A job whose pod can't pull its image for longer than
  `--pod-timeout` is marked `Failed` (reason `DeadlineExceeded`) instead of
  being deleted, and a job with the condition `FailureTarget` (Kubernetes
  1.31 and later) counts as failed. The TESK API can then report the
  executor as failed (`EXECUTOR_ERROR`) and show its logs. Clean-up of
  finished jobs is up to the deployment, as before for jobs that failed.
- **Executors aren't retried by default.** Executor jobs get
  `backoffLimit: 0` unless the job or `EXECUTOR_BACKOFF_LIMIT` sets a limit.
  Before, Kubernetes' default of 6 retries applied. TESK's Helm chart sets
  `EXECUTOR_BACKOFF_LIMIT` from `tesk.executorRetries` (2 by default), so chart
  deployments keep that limit; set it to 0 for single runs. Without retries,
  an executor whose pod is evicted (e.g., from a drained node) fails.
- **Permissions.** The taskmaster now patches jobs (to fail them). The
  taskmaster role of TESK's Helm chart already allows `patch` on `jobs`; if
  you use your own role, add it. Without it, the taskmaster deletes such jobs,
  as before.

#### Upgrade order

Upgrade the TESK API first, to the release that derives the task state from
all jobs of a task ([tesk-api#104](https://github.com/elixir-cloud-aai/tesk-api/issues/104)),
then the taskmaster. That release reads the conditions `Failed` and
`FailureTarget` and checks that every executor of a task ran. Tasks running
during the upgrade keep the taskmaster they were started with.

While only one of them is upgraded:

- An older TESK API with this taskmaster sees a failed executor's job, as it
  is kept, and reports `EXECUTOR_ERROR`. For a few seconds after Kubernetes
  sets `FailureTarget` and before it counts the failed pod, the older TESK
  API can report `QUEUED` or `SYSTEM_ERROR` for such a task.
- The newer TESK API with an older taskmaster reports a task whose last
  executor's job the taskmaster deleted as `SYSTEM_ERROR`, since an executor
  is missing.
