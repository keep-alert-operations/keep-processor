# syntax=docker/dockerfile:1
FROM python:3.13-slim-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir uv==0.10.8
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src ./src
COPY scripts/git-askpass.sh /tmp/git-askpass.sh
RUN chmod 700 /tmp/git-askpass.sh
RUN --mount=type=secret,id=github_token GIT_ASKPASS=/tmp/git-askpass.sh GIT_TERMINAL_PROMPT=0 uv sync --locked --no-dev --no-editable

FROM python:3.13-slim-bookworm AS runtime
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 APPCHI_BIND_HOST=0.0.0.0
RUN chgrp -R 0 /app && chmod -R g=rX /app
USER 1001
CMD ["python", "-m", "appchi.processor.main"]
