# Adapted from https://github.com/astral-sh/uv-docker-example/blob/main/Dockerfile
# NOTE: the multi-stage build version with smaller final image (https://github.com/astral-sh/uv-docker-example/blob/main/multistage.Dockerfile)
# is NOT usable with the deploy() command (using this Dockerfile as base image) for prefect v3 flows (prefect.utilities.dockerutils.BuildError: the --mount option requires BuildKit. Refer to https://docs.docker.com/go/buildkit/ to learn how to build images with BuildKit enabled)
# IIUC, this is due to a limitation of an underlying dependency: https://github.com/PrefectHQ/prefect/issues/12922
# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Install the project's dependencies using the lockfile and settings
RUN --mount=type=cache,target=/root/.cache/uv \
  --mount=type=bind,source=uv.lock,target=uv.lock \
  --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
  uv sync --locked --no-install-project --no-dev

# Then, add the rest of the project source code and install it
# Installing separately from its dependencies allows optimal layer caching
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --locked --no-dev

# Place executables in the environment at the front of the path
ENV PATH="/app/.venv/bin:$PATH"

# Reset the entrypoint, don't invoke `uv`
ENTRYPOINT []