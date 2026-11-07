# Upgrading

## To the next release

- **Failed jobs are kept.** The taskmaster no longer deletes a job that
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

### Upgrade order

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
