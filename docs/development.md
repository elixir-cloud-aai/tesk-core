# Development

The code is the Python package `teskcore` in `src/tesk_core/`, with the
commands `taskmaster` and `filer`. Contributions go as pull requests to the
branch `dev` of
[elixir-cloud-aai/tesk-core](https://github.com/elixir-cloud-aai/tesk-core);
see the
[contributing guidelines](https://github.com/elixir-cloud-aai/.github/blob/main/CONTRIBUTING.md)
of ELIXIR Cloud & AAI.

## Images

Each command has a `Dockerfile` in `containers/`, which builds the package
as a wheel and installs it on Alpine, with the runtime dependencies at the
versions of `uv.lock`, checked against their hashes. Build them from the
root of the repository, which needs the Git history for the version
(`setuptools_scm`):

```bash
docker build --file containers/taskmaster.Dockerfile --tag tesk-core-taskmaster:local .
docker build --file containers/filer.Dockerfile --tag tesk-core-filer:local .
```

Without the Git history (e.g., from a source archive or a Git worktree,
whose `.git` points outside the build context), the images report version
`0.0.0.dev0`; set the version with `--build-arg
SETUPTOOLS_SCM_PRETEND_VERSION=<version>`, as the release workflow does.

To try them in a TESK deployment, push them to a registry that the cluster
can pull from, and set the image names and tags in TESK's Helm chart (see
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation).

## Dependencies

`pyproject.toml` holds the package's metadata and dependencies: the runtime
dependencies, and the extras `dev` (tests, lint, type checks) and `docs`.
[uv](https://docs.astral.sh/uv/) locks them all in `uv.lock`, which CI
checks; after changing a dependency, update the lock with `uv lock`.

## Tests and lint

The tests use pytest and are split by level:

- `tests/unit/`: the modules on their own, with Kubernetes, S3 (moto) and
  the network mocked; the files they read lie in `tests/unit/resources/`.
- `tests/integration/`: the FTP and HTTP transfers of the filer against
  local servers (`pytest-localftpserver` and a local HTTP server), started
  by the tests.
- `tests/e2e/`: tasks run by the taskmaster's and the filer's images on a
  Kubernetes cluster (see [End-to-end tests](#end-to-end-tests)); skipped
  unless `TESK_E2E` is set.

With uv, which creates `.venv` with the locked versions:

```bash
uv sync --extra dev
uv run coverage run -m pytest
uv run coverage report -m
```

The tests must cover every line of the package (line coverage, not
branches): `coverage report` fails below 100% (`fail_under` in
`pyproject.toml`), and Codecov requires 100% of the code and of each pull
request's changes (`codecov.yml`). Only the script entry points
(`if __name__ == "__main__":`) are left out.

`uv run pytest tests/unit` or `uv run pytest tests/integration` runs one
level. New tests are plain pytest functions `test_*` (or methods
`test_*` of classes `Test*` without `unittest.TestCase`; pytest collects
nothing else) in a file of the matching level named after the module or
topic (e.g. `test_filer_s3.py`); they are linted and type-checked like the
package.

[ruff](https://docs.astral.sh/ruff/) lints and formats the code, and
[mypy](https://mypy.readthedocs.io/) checks its type hints (complete type
hints and Google-style docstrings in `src/`). Both run with the other
checks of `.pre-commit-config.yaml` (file checks, actionlint, gitleaks); run
them on every commit with `uv run pre-commit install`, or once:

```bash
uv run pre-commit run --all-files
```

### End-to-end tests

The end-to-end tests run tasks as the TESK API does: one taskmaster job per
task, with the task's JSON and its pod's labels, in a namespace with the
taskmaster's service account and role (the rules of TESK's Helm chart) and
a small HTTP server for inputs and outputs (`tests/e2e/manifests/`). They
check the resulting jobs, PVCs and uploads: a task with inputs and outputs,
a failing executor, a cancelled task, and a task that can't be scheduled
(`TESK_MAX_QUEUE_SECONDS`). The images run with `imagePullPolicy: Never`,
and the filer image also runs the executors and the HTTP server, so no
other image is pulled. The unit tests in `tests/unit/test_manifests.py`
check the manifests against the Kubernetes client's models without a
cluster.

With [kind](https://kind.sigs.k8s.io/) and Docker, from the root of the
repository (a separate kubeconfig, outside the repository and so outside
the images' build context, leaves your own untouched; the tests use its
current context, which `kind create cluster` sets):

```bash
export KUBECONFIG="$HOME/.kube/kind-tesk-core-e2e"
kind create cluster --name tesk-core-e2e
docker build --file containers/taskmaster.Dockerfile --tag tesk-core-taskmaster:e2e .
docker build --file containers/filer.Dockerfile --tag tesk-core-filer:e2e .
kind load docker-image tesk-core-taskmaster:e2e tesk-core-filer:e2e --name tesk-core-e2e
kubectl apply -f tests/e2e/manifests/
kubectl wait --for=condition=Ready pod/tesk-e2e-http --timeout=120s
TESK_E2E=1 uv run pytest tests/e2e
kind delete cluster --name tesk-core-e2e
rm "$KUBECONFIG"
```

The tests use the namespace `default`; for another one, apply the manifests
and wait for the server with `-n <namespace>`, and set
`TESK_E2E_NAMESPACE`. The image
names are fixed (`tests/e2e/cluster.py`). The role in
`tests/e2e/manifests/rbac.yaml` grants what the role of TESK's chart
grants: update both together.

## Continuous integration

The workflow `CI` (`.github/workflows/ci.yml`) checks every pull request to
`dev` and every push to it:

| Job | Checks |
|---|---|
| `PR title` | The title follows Conventional Commits, at most 50 characters (squash merges use it as the commit message). |
| `Lint` | The hooks of `.pre-commit-config.yaml` (ruff, mypy, actionlint, gitleaks, file checks), the lock file (`uv lock --check`), and the citation metadata (`CITATION.cff`, cffconvert). |
| `Test (Python …)` | The tests on Python 3.10 to 3.13, with coverage to Codecov. |
| `End-to-end test` | The end-to-end tests on a kind cluster, with both images built from the commit (see [End-to-end tests](#end-to-end-tests)). |
| `Docs` | These docs, built strictly. |
| `Scan image (…)` | Trivy scan of the taskmaster and filer images: fails on critical and high vulnerabilities that have a fix. Accepted findings are listed in `.trivyignore.yaml`, with a reason and an expiry date. |
| `CI passed` | The gate: the only required check. |

On pushes to `dev`, the jobs `Push image (…)` and `Publish image (…)` push
both images by digest, scan exactly those digests, then attest, sign
(Sigstore) and tag them `dev` and `sha-<commit>`, in the environment
`dev-images` (secrets `DOCKERHUB_LOGIN` and `DOCKERHUB_TOKEN`; the tests
upload coverage with `CODECOV_TOKEN`). A fork publishes to its own Docker
Hub organization if it sets the variable `DOCKERHUB_ORG`.

The workflow `Scan` (`.github/workflows/scan.yml`) scans the `latest` images
weekly, so that vulnerabilities found after a release are reported to code
scanning, with a full report as a workflow artifact. Until the first release
of the release workflow below, `latest` is still the image of v0.10.6 from
the old tag workflows, and the scan only reports; after that release, it
fails on critical and high vulnerabilities that have a fix.

## Versioning and releases

Pull requests go to `dev` (squash merges; titles in the [Conventional
Commits](https://www.conventionalcommits.org/) format, at most 50
characters). For a release, `main` is moved forward to a commit of `dev`.
Pushing a signed, annotated version tag runs the workflow `Release`
(`.github/workflows/release.yml`), which publishes both images to Docker Hub,
as `<org>/tesk-core-taskmaster` and `<org>/tesk-core-filer`:

| Tag | Image tags | Example |
|---|---|---|
| `vX.Y.Z` | `X.Y.Z`, and `latest` if it is the highest release on `main` | `v0.11.0` publishes `0.11.0` |
| `vX.Y.ZrcN` (also `aN`, `bN`) | `X.Y.ZrcN` only | `v0.11.0rc1` publishes `0.11.0rc1` |

The image tags have no `v`, as those of tesk-api; the old tag workflows
published `vX.Y.Z` (up to `v0.10.6`), so set the new form in TESK's Helm
chart when moving to a new release. The package version in the images is
the tag's (without `v`; checked in the images). Before tagging, set the
release in a pull request:

- `CITATION.cff`: `version`, the tag without `v` (e.g., `0.11.0rc1` for
  `v0.11.0rc1`), and `date-released`.

The release workflow first checks, without running any code of the tagged
commit, that the tag is a release version, annotated and signed (with a
signature that GitHub verified), and on `main`; then that `CITATION.cff` has
the version and that CI passed for the commit's push to `dev`. It then builds
both images and pushes them by digest, untagged; scans exactly those digests
(failing on critical and high vulnerabilities that have a fix, except those
accepted in `.trivyignore.yaml`), in a job without credentials; and only then
attests their build provenance, signs them (Sigstore) and tags them. Pushing
and publishing each wait for an approval in the environment `release`, which
only allows `v*` tags and has the secrets `DOCKERHUB_LOGIN` and
`DOCKERHUB_TOKEN`. A version that is published never moves to another
digest: if a release fails after publishing, follow it with a higher version
(`latest` goes by tags). Only admins can create release tags, and nobody can
move or delete them (tag rulesets). The workflow doesn't create a GitHub
release; write its notes in the format of the organisation's other repos.

After the first release of this workflow, drop `fail: "false"` in
`.github/workflows/scan.yml`, and the note on it above.

## Dependencies and metadata

Dependabot (`.github/dependabot.yml`) opens grouped pull requests to `dev`
once a month for the dependencies in `pyproject.toml` and `uv.lock` (the
docs' dependencies in pull requests of their own), the actions in the
workflows, the images in `containers/` and the pre-commit hooks.
`.github/CODEOWNERS` names the code owners, who are requested for review on
every pull request. `CITATION.cff` holds the
citation metadata, with the version and date of the latest release; CI
validates it.

## Docs

These docs are Markdown pages in `docs/`, built with MkDocs and the Material
theme (`mkdocs.yml`). They are published as the section "tesk-core" of the
[TESK documentation](https://tesk.readthedocs.io/), which pulls `docs/` from
this repository at the commit pinned in TESK's `docs-sources.yml`; nothing
is published from here. So:

- link to other pages of `docs/` with relative links, and to everything
  else, including the rest of the TESK documentation, with full URLs;
- use only the Markdown extensions of this `mkdocs.yml`, which TESK's site
  has as well;
- list new pages in `nav` of `mkdocs.yml`, which also sets their order in
  TESK's site.

Build them strictly, as CI does:

```bash
uv sync --extra docs
uv run mkdocs build --strict
```

To publish a change, update the `ref` of `tesk-core` in TESK's
`docs-sources.yml` to the commit on `dev` (see "Docs of tesk-api and
tesk-core" in TESK's
[development docs](https://tesk.readthedocs.io/en/latest/development/)).
