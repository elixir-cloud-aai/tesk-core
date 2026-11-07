# Upgrading

Changes that operators must act on when they upgrade the taskmaster and the
filer. Upgrade both images together: the taskmaster passes the settings on
to the filer.

## Unreleased

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
