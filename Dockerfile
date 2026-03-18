FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    SILKWORM_MCP_DOCUMENT_MAX_COUNT=128 \
    SILKWORM_MCP_DOCUMENT_MAX_TOTAL_BYTES=32000000 \
    SILKWORM_MCP_DOCUMENT_TTL_SECONDS=3600 \
    SILKWORM_MCP_READINESS_REQUIRE_CDP=true \
    PATH="/app/.venv/bin:${PATH}"

COPY --from=ghcr.io/astral-sh/uv:0.9.16 /uv /uvx /bin/
COPY --from=rust:1.94-slim /usr/local/cargo /usr/local/cargo
COPY --from=rust:1.94-slim /usr/local/rustup /usr/local/rustup

WORKDIR /app

RUN apt-get update && \
    apt-get install --yes --no-install-recommends build-essential ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

RUN groupadd --system app && \
    useradd --system --gid app --create-home --home-dir /app app

RUN curl -L -o /usr/local/bin/lightpanda https://github.com/lightpanda-io/browser/releases/download/nightly/lightpanda-x86_64-linux && \
    chmod 0755 /usr/local/bin/lightpanda

COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY mcp_server.py ./
COPY docker-entrypoint.sh ./
RUN uv sync --frozen --no-dev
RUN chmod 0755 /app/docker-entrypoint.sh
RUN chown -R app:app /app

USER app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/readyz >/dev/null || exit 1

CMD ["./docker-entrypoint.sh"]
