# Upgrading

## To the next release

- **Failed jobs are kept.** The taskmaster no longer deletes a job that
  failed. A job whose pod can't pull its image for longer than
  `--pod-timeout` is marked `Failed` (reason `DeadlineExceeded`) instead of
  being deleted, and a job with the condition `FailureTarget` (Kubernetes
  1.31 and later) counts as failed. The TESK API can then report the
  executor as failed (`EXECUTOR_ERROR`) and show its logs. Only a cancelled
  task's running job is deleted. Clean-up of finished jobs is up to the
  deployment, as before for jobs that failed.
- **Executors aren't retried.** Executor jobs get `backoffLimit: 0` unless
  the job or `EXECUTOR_BACKOFF_LIMIT` sets a limit. Before, Kubernetes' default
  of 6 retries applied. Set `EXECUTOR_BACKOFF_LIMIT` to keep retrying.
- **Permissions.** The taskmaster now patches jobs (to fail them). The
  taskmaster role of TESK's Helm chart already allows `patch` on `jobs`; if
  you use your own role, add it.

### Upgrade order

Upgrade the TESK API to the release that reports failed executors as
`EXECUTOR_ERROR` (tesk-api#104) first, then the taskmaster. Either order
works without reporting tasks worse than before:

- An older TESK API with this taskmaster reports a failed executor as
  `EXECUTOR_ERROR`, because the failed job is now kept, where it could report
  `COMPLETE` before.
- The newer TESK API with an older taskmaster reports a task whose executor
  job the taskmaster deleted as `SYSTEM_ERROR` instead of `COMPLETE`, because
  an executor is missing.

Tasks that are running during the upgrade keep the taskmaster they were
started with.
