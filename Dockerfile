# syntax=docker/dockerfile:1
FROM python:3.13-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
RUN python -m venv /opt/poetry && /opt/poetry/bin/python -m pip install --no-cache-dir poetry==2.3.1
ENV PATH="/opt/poetry/bin:$PATH" POETRY_VIRTUALENVS_IN_PROJECT=true POETRY_SYSTEM_GIT_CLIENT=true
WORKDIR /app
COPY pyproject.toml poetry.lock poetry.toml ./
COPY src ./src
COPY scripts/git-askpass.sh /tmp/git-askpass.sh
RUN chmod 700 /tmp/git-askpass.sh
RUN --mount=type=secret,id=github_token \
    poetry check --lock && \
    GIT_ASKPASS=/tmp/git-askpass.sh GIT_TERMINAL_PROMPT=0 poetry --no-cache sync --only main --no-root && \
    poetry build --format wheel && \
    .venv/bin/python -m pip install --no-deps --no-cache-dir dist/*.whl

FROM python:3.13-slim-bookworm AS runtime
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 APPCHI_BIND_HOST=0.0.0.0
RUN chgrp -R 0 /app && chmod -R g=rX /app
USER 1001
CMD ["python", "-m", "appchi.processor.main"]
