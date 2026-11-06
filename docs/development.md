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
as a wheel and installs it on Alpine. Build them from the root of the
repository, which needs the Git history for the version (`setuptools_scm`):

```bash
docker build --file containers/taskmaster.Dockerfile --tag tesk-core-taskmaster:local .
docker build --file containers/filer.Dockerfile --tag tesk-core-filer:local .
```

To try them in a TESK deployment, push them to a registry that the cluster
can pull from, and set the image names and tags in TESK's Helm chart (see
[Configuration](https://tesk.readthedocs.io/en/latest/configuration/) in the
TESK documentation).

## Tests and lint

The tests in `tests/` use pytest. With a virtual environment:

```bash
python -m pip install --editable ".[test]" coverage
coverage run --source=tesk_core -m pytest tests
coverage report -m
python -m pip install pylint
python -m pylint --errors-only tesk_core
```

`tox` runs the same on every Python version that it finds (3.10 to 3.13).

## Continuous integration

The workflow `CI` (`.github/workflows/ci.yml`) checks every pull request to
`dev` and every push to it:

| Job | Checks |
|---|---|
| `PR title` | The title follows Conventional Commits, at most 50 characters (squash merges use it as the commit message). |
| `Lint` | The workflows (actionlint), and the package with pylint (errors only). |
| `Test (Python …)` | The tests on Python 3.10 to 3.13, with coverage to Codecov. |
| `Docs` | These docs, built strictly. |
| `Scan image (…)` | Trivy scan of the taskmaster and filer images: fails on critical and high vulnerabilities that have a fix. Accepted findings are listed in `.trivyignore.yaml`, with a reason and an expiry date. |
| `CI passed` | The gate: the only required check. |

On pushes to `dev`, the jobs `Push image (…)` and `Publish image (…)` push
both images by digest, scan exactly those digests, then attest, sign
(Sigstore) and tag them `dev` and `sha-<commit>`, in the environment
`dev-images` (secrets `DOCKERHUB_LOGIN` and `DOCKERHUB_TOKEN`; the tests
upload coverage with `CODECOV_TOKEN`). A fork publishes to its own Docker
Hub organization if it sets the variable `DOCKERHUB_ORG`.

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
python -m pip install --requirement docs/requirements.txt
mkdocs build --strict
```

To publish a change, update the `ref` of `tesk-core` in TESK's
`docs-sources.yml` to the commit on `dev` (see "Docs of tesk-api and
tesk-core" in TESK's
[development docs](https://tesk.readthedocs.io/en/latest/development/)).
