# Upgrading

Changes that operators must act on when they upgrade the taskmaster and the
filer. Upgrade both images together: the taskmaster passes the settings on
to the filer.

## Unreleased

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
be scheduled, still wait. No change is needed.

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
