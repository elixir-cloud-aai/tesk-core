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
chart when moving to a new release. The package version is derived from the
tag (`setuptools_scm`). Before tagging, set the release in a pull request:

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
