[![CI](https://github.com/elixir-cloud-aai/tesk-core/actions/workflows/ci.yml/badge.svg?branch=dev)](https://github.com/elixir-cloud-aai/tesk-core/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/elixir-cloud-aai/tesk-core/branch/dev/graph/badge.svg)](https://codecov.io/gh/elixir-cloud-aai/tesk-core)

## Introduction

This project is part of the [TESK](https://github.com/EMBL-EBI-TSI/TESK) initiative.
It contains the code needed to generate 2 types of agents that reside in kubernetes:
* The taskmaster, which spins up the containers needed to complete tasks as defined by TESK
* The filer, which populates volumes and input files and uploads output files

## How to use

Since the code is meant to be in kubernetes pods, the code needs to be packaged into containers.
Their descriptions can be found in `containers/`.
The root folder assumed to build the containers is the root of this package.

To build the taskmaster container, run:

```
docker build . -f containers/taskmaster.Dockerfile -t taskmaster:latest
```

The command is similar for the filer container:

```
docker build . -f containers/filer.Dockerfile -t filer:latest
```

## Unit testing

Unit testing needs the `tox` package.

You can install the package using `uv`:

```
uv install tox
```

To install different python versions using `uv`, you can type:

```
uv python install 3.10 3.11 3.12 3.13
```

This software will take care of creating virtual environments and installing dependencies in them before running the actual tests and generating the coverage reports.

```
$ uv run tox
```

## Continuous integration

The workflow `CI` (`.github/workflows/ci.yml`) checks every pull request to
`dev` and every push to it: the pull request title, lint (the workflows, and
pylint for errors), the tests on Python 3.10 to 3.13 (coverage to Codecov),
and a vulnerability scan (Trivy) of the taskmaster and filer images. Its job
`CI passed` is the only required check. On pushes to `dev`, the images are
pushed, scanned, signed and tagged `dev` and `sha-<commit>`:
`docker.io/elixircloud/tesk-core-taskmaster` and
`docker.io/elixircloud/tesk-core-filer`. Accepted vulnerabilities are listed
in `.trivyignore.yaml`, with a reason and an expiry date. Tags no longer
publish images, until the release workflow is added (#64); the Travis and
Cloud Build (`eu.gcr.io`) setups were removed. `tox` runs the same tests and
lint locally.

Publishing needs the environment `dev-images` (branch `dev` only) with the
secrets `DOCKERHUB_LOGIN` and `DOCKERHUB_TOKEN`; the tests upload coverage
with `CODECOV_TOKEN`. A fork publishes to its own Docker Hub organization if
it sets the variable `DOCKERHUB_ORG`.
