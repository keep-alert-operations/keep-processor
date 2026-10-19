# syntax=docker/dockerfile:1
ARG PYTHON_BASE_IMAGE=python:3.13-slim-bookworm
FROM ${PYTHON_BASE_IMAGE} AS build
# build_ca contains public PEM CA certificates only; apt still verifies signatures.
RUN --mount=type=secret,id=apt_sources --mount=type=secret,id=build_ca \
    set -eu; \
    if [ -f /run/secrets/apt_sources ]; then \
        rm -f /etc/apt/sources.list /etc/apt/sources.list.d/*; \
        cp /run/secrets/apt_sources /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /run/secrets/build_ca ]; then \
        if grep -q 'PRIVATE KEY' /run/secrets/build_ca; then exit 1; fi; \
        mkdir -p /usr/local/share/ca-certificates /etc/ssl/certs; \
        cp /run/secrets/build_ca /usr/local/share/ca-certificates/appchi-build-ca.crt; \
        cat /run/secrets/build_ca >> /etc/ssl/certs/ca-certificates.crt; \
    fi; \
    apt-get update && apt-get install -y --no-install-recommends git ca-certificates && \
    update-ca-certificates && rm -rf /var/lib/apt/lists/*
RUN --mount=type=secret,id=build_environment \
    set -eu; \
    if [ -f /run/secrets/build_environment ]; then \
        set -a; . /run/secrets/build_environment; set +a; \
    fi; \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    export PIP_CERT="$SSL_CERT_FILE" REQUESTS_CA_BUNDLE="$SSL_CERT_FILE"; \
    export GIT_SSL_CAINFO="$SSL_CERT_FILE" NODE_EXTRA_CA_CERTS="$SSL_CERT_FILE"; \
    python -m venv /opt/poetry; \
    /opt/poetry/bin/python -m pip install --no-cache-dir poetry==2.3.1
ENV PATH="/opt/poetry/bin:$PATH" POETRY_VIRTUALENVS_IN_PROJECT=true POETRY_SYSTEM_GIT_CLIENT=true
WORKDIR /app
COPY pyproject.toml poetry.lock poetry.toml ./
COPY src ./src
COPY scripts/git-askpass.sh /tmp/git-askpass.sh
RUN chmod 700 /tmp/git-askpass.sh
RUN --mount=type=secret,id=github_token --mount=type=secret,id=build_environment \
    set -eu; \
    if [ -f /run/secrets/build_environment ]; then \
        set -a; . /run/secrets/build_environment; set +a; \
    fi; \
    export SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt; \
    export PIP_CERT="$SSL_CERT_FILE" REQUESTS_CA_BUNDLE="$SSL_CERT_FILE"; \
    export GIT_SSL_CAINFO="$SSL_CERT_FILE" NODE_EXTRA_CA_CERTS="$SSL_CERT_FILE"; \
    export GIT_CONFIG_GLOBAL=/tmp/appchi-build-gitconfig; \
    if [ -n "${APPCHI_GIT_BASE_URL:-}" ]; then \
        case "$APPCHI_GIT_BASE_URL" in https://*/ ) ;; *) exit 1 ;; esac; \
        case "$APPCHI_GIT_BASE_URL" in *'@'*|*'?'*|*'#'*) exit 1 ;; esac; \
        git config --global url."$APPCHI_GIT_BASE_URL".insteadOf https://github.com/appchi-alert-operations/; \
        git config --global --add url."$APPCHI_GIT_BASE_URL".insteadOf ssh://git@github.com/appchi-alert-operations/; \
    else \
        git config --global url."https://github.com/".insteadOf ssh://git@github.com/; \
    fi; \
    poetry check --lock; \
    GIT_ASKPASS=/tmp/git-askpass.sh GIT_TERMINAL_PROMPT=0 poetry --no-cache sync --only main --no-root; \
    poetry build --format wheel; \
    .venv/bin/python -m pip install --no-deps --no-cache-dir dist/*.whl; \
    rm -f "$GIT_CONFIG_GLOBAL"

FROM ${PYTHON_BASE_IMAGE} AS runtime
# The locked Linux CEL wheel dynamically links libstdc++.so.6.
# build_ca contains public PEM CA certificates only; apt still verifies signatures.
RUN --mount=type=secret,id=apt_sources --mount=type=secret,id=build_ca \
    set -eu; \
    if [ -f /run/secrets/apt_sources ]; then \
        rm -f /etc/apt/sources.list /etc/apt/sources.list.d/*; \
        cp /run/secrets/apt_sources /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /run/secrets/build_ca ]; then \
        if grep -q 'PRIVATE KEY' /run/secrets/build_ca; then exit 1; fi; \
        mkdir -p /usr/local/share/ca-certificates /etc/ssl/certs; \
        cp /run/secrets/build_ca /usr/local/share/ca-certificates/appchi-build-ca.crt; \
        cat /run/secrets/build_ca >> /etc/ssl/certs/ca-certificates.crt; \
    fi; \
    apt-get update && apt-get install -y --no-install-recommends libstdc++6 ca-certificates && \
    update-ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 APPCHI_BIND_HOST=0.0.0.0
RUN chgrp -R 0 /app && chmod -R g=rX /app
USER 1001:0
# Compile a checked native expression without contacting configured services.
RUN python -c "import appchi.processor.main; from cel_expr_python import cel; assert cel.NewEnv().compile('true').return_type() == cel.Type.BOOL"
CMD ["python", "-m", "appchi.processor.main"]
