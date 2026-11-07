# tesk-core

tesk-core holds the two programs that run a task in
[TESK](https://tesk.readthedocs.io/), the GA4GH Task Execution Service on
Kubernetes:

- The [taskmaster](taskmaster.md) runs one task: for each task that the TESK
  API accepts, it runs as a Kubernetes job of its own, creates the task's
  volume, has the filer stage the inputs, runs the executors one after
  another, has the filer stage the outputs, and cleans up.
- The [filer](filer.md) stages a task's files: it downloads the inputs onto
  the task's volume before the executors run, and uploads the outputs after
  them, over HTTP(S), FTP, S3, or a shared volume (`file:`).

Both are commands of the Python package `teskcore` (`taskmaster` and `filer`),
and are shipped as one image each:

| Image | Command |
|---|---|
| `docker.io/elixircloud/tesk-core-taskmaster` | `taskmaster` |
| `docker.io/elixircloud/tesk-core-filer` | `filer` |

The images of the `dev` branch are tagged `dev` and `sha-<commit>`. The TESK
API starts the taskmaster with the image that its configuration names, and
the taskmaster starts the filer with the image that the API passes on to it.
Deploy TESK with its Helm chart, which sets both: see
[Deployment](https://tesk.readthedocs.io/en/latest/deployment/) and
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation.

- [Taskmaster](taskmaster.md): what it does, its options and environment
  variables.
- [Filer](filer.md): protocols, credentials, and environment variables.
- [Upgrading](upgrading.md): changes that operators must act on.
- [Development](development.md): building the images, tests, lint, CI, and
  these docs.
