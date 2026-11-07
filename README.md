<!-- markdownlint-disable MD033 MD041 -- logo first; <picture> for dark mode -->
<a href="https://elixir-on.cloud/">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/images/logo_elixir_on_cloud_night.png">
    <img src="docs/images/logo_elixir_on_cloud.png" alt="ELIXIR On-Cloud">
  </picture>
</a>
<!-- markdownlint-enable MD033 MD041 -->

# tesk-core

[![License][badge-license]][badge-url-license]
[![Build_status][badge-build-status]][badge-url-build-status]
[![Docs][badge-docs]][badge-url-docs]
[![Coverage][badge-coverage]][badge-url-coverage]
[![GitHub_tag][badge-github-tag]][badge-url-github-tag]
[![Taskmaster_image][badge-docker-taskmaster]][badge-url-docker-taskmaster]
[![Filer_image][badge-docker-filer]][badge-url-docker-filer]

The taskmaster and the filer of [TESK][tesk], an implementation of the
[GA4GH Task Execution Service (TES) API][tes] for
[Kubernetes](https://kubernetes.io), brought to you by
[ELIXIR On-Cloud](https://elixir-on.cloud/). For each task that the TESK
API accepts, the **taskmaster** runs as a Kubernetes job of its own: it
creates the task's volume, has the filer stage the inputs, runs the
executors one after another, has the filer stage the outputs, and cleans
up. The **filer** downloads the inputs onto the task's volume and uploads
the outputs, over HTTP(S), FTP, S3 or a shared volume. Both are commands of
the Python package `teskcore`, published as the images
`elixircloud/tesk-core-taskmaster` and `elixircloud/tesk-core-filer`, and
deployed with the Helm chart of [TESK][tesk].

**Read the [documentation][docs]** (the "tesk-core" section of the TESK
documentation) for the taskmaster, the filer and development. Its sources
are in [`docs/`](docs/).

## Quick start

The taskmaster needs a Kubernetes cluster and the TESK API: deploy TESK
with its Helm chart, which sets both images. The filer also runs on its
own. With [Docker](https://docs.docker.com/get-docker/) installed, download
a file into the current directory, as the filer does for a task's input:

```bash
docker run --rm --user "$(id -u)" --volume "${PWD}:/data" \
  elixircloud/tesk-core-filer:dev inputs \
  '{"inputs": [{"url": "https://raw.githubusercontent.com/elixir-cloud-aai/tesk-core/dev/LICENSE", "path": "/data/LICENSE", "type": "FILE"}]}'
```

## Development

The package needs Python 3.10 or later. With
[uv](https://docs.astral.sh/uv/), install it with the locked development
dependencies and run the tests:

```bash
uv sync --extra dev
uv run pytest
```

See [Development][docs-development] in the documentation for the images,
lint, CI and the docs build.

## Citation

If you use tesk-core, please cite it with the metadata in
[`CITATION.cff`](CITATION.cff): on GitHub, "Cite this repository" in the
sidebar of the repository formats it as APA or BibTeX.

## Contributing

Contributions are welcome! See the organisation's [contributing
guide][contributing], and mind our [code of conduct][code-of-conduct].

[badge-build-status]: <https://github.com/elixir-cloud-aai/tesk-core/actions/workflows/ci.yml/badge.svg?branch=dev>
[badge-coverage]: <https://codecov.io/gh/elixir-cloud-aai/tesk-core/branch/dev/graph/badge.svg?branch=dev>
[badge-docker-filer]: <https://img.shields.io/docker/v/elixircloud/tesk-core-filer?sort=semver&label=filer&color=C39BD3>
[badge-docker-taskmaster]: <https://img.shields.io/docker/v/elixircloud/tesk-core-taskmaster?sort=semver&label=taskmaster&color=C39BD3>
[badge-docs]: <https://img.shields.io/readthedocs/tesk>
[badge-github-tag]: <https://img.shields.io/github/v/tag/elixir-cloud-aai/tesk-core?color=C39BD3>
[badge-license]: <https://img.shields.io/badge/license-Apache%202.0-blue.svg>
[badge-url-build-status]: <https://github.com/elixir-cloud-aai/tesk-core/actions/workflows/ci.yml?query=branch%3Adev>
[badge-url-coverage]: <https://codecov.io/gh/elixir-cloud-aai/tesk-core?branch=dev>
[badge-url-docker-filer]: <https://hub.docker.com/r/elixircloud/tesk-core-filer/tags>
[badge-url-docker-taskmaster]: <https://hub.docker.com/r/elixircloud/tesk-core-taskmaster/tags>
[badge-url-docs]: <https://tesk.readthedocs.io/en/latest/tesk-core/>
[badge-url-github-tag]: <https://github.com/elixir-cloud-aai/tesk-core/tags>
[badge-url-license]: <https://www.apache.org/licenses/LICENSE-2.0>
[code-of-conduct]: <https://github.com/elixir-cloud-aai/.github/blob/main/CODE_OF_CONDUCT.md>
[contributing]: <https://github.com/elixir-cloud-aai/.github/blob/main/CONTRIBUTING.md>
[docs]: <https://tesk.readthedocs.io/en/latest/tesk-core/>
[docs-development]: <https://tesk.readthedocs.io/en/latest/tesk-core/development/>
[tes]: <https://github.com/ga4gh/task-execution-schemas>
[tesk]: <https://github.com/elixir-cloud-aai/TESK>
