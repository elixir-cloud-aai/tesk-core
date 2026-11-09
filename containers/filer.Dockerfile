# The filer image. Build from the root of the repository, with its Git
# history (for the version), e.g.:
#   docker build --file containers/filer.Dockerfile --tag tesk-core-filer:local .
# `taskmaster.Dockerfile` is the same but for the user and the entrypoint.

# uv, to export the locked requirements (`uv.lock`), build the wheel and
# install both; mounted into the steps that use it only. Pinned by digest, as
# the base image; Dependabot updates the tags and digests together
FROM ghcr.io/astral-sh/uv:0.13.7@sha256:96f40d3904aad8d0bd010539001d36c8e2188b981e1334471a90c4a1d93bb490 AS uv

# Builder: the wheel of the package, and the locked runtime requirements
FROM alpine:3.23@sha256:f3d55900449227ae9e237ede521e7b665d53a253b42065ba90b9db9275e198ad AS builder

RUN apk add --no-cache python3 git

WORKDIR /app/
COPY . .

# The package version; by default derived from Git (`setuptools_scm`), set
# by the release workflow to the release tag's
ARG SETUPTOOLS_SCM_PRETEND_VERSION
# The runtime dependencies (`project.dependencies`) and the build
# requirements (dependency group `build-system`) at the versions of
# `uv.lock`, with their hashes (`--frozen`: CI checks that the lock matches
# `pyproject.toml`). The build requirements are installed hash-checked, as
# wheels only, into a virtual environment of their own, and the wheel is
# built with them, without an isolated build environment (which would be
# filled with downloads that aren't hash-checked) and offline
RUN --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
  export UV_NO_CACHE=1 UV_PYTHON_DOWNLOADS=never \
  && uv export --frozen --no-dev --no-emit-project \
    --output-file /tmp/requirements.txt \
  && uv export --frozen --only-group build-system --no-emit-project \
    --output-file /tmp/build-requirements.txt \
  && uv venv --python /usr/bin/python3 /tmp/build-env \
  && uv pip install --python /tmp/build-env --require-hashes \
    --only-binary :all: --no-deps -r /tmp/build-requirements.txt \
  && uv build --wheel --offline --no-build-isolation \
    --python /tmp/build-env/bin/python --out-dir /tmp/dist

# Install: the locked requirements, checked against their hashes, as wheels
# only and without resolving anything else, then the package without its
# dependencies, offline; then check that they meet each other's requirements
FROM alpine:3.23@sha256:f3d55900449227ae9e237ede521e7b665d53a253b42065ba90b9db9275e198ad

RUN apk add --no-cache python3

RUN --mount=from=builder,source=/tmp,target=/tmp/build \
  --mount=from=uv,source=/uv,target=/usr/local/bin/uv \
  export UV_NO_CACHE=1 UV_PYTHON_DOWNLOADS=never \
  && uv pip install --system --break-system-packages --require-hashes \
    --only-binary :all: --no-deps --compile-bytecode \
    -r /tmp/build/requirements.txt \
  && uv pip install --system --break-system-packages --offline --no-index \
    --no-deps --compile-bytecode /tmp/build/dist/*.whl \
  && uv pip check --system \
  && test -x /usr/bin/filer

USER 100

# Absolute, so that it doesn't depend on `PATH` and the directories on it;
# the build fails above if the script is elsewhere
ENTRYPOINT ["/usr/bin/filer"]
