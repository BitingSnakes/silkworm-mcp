FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
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

COPY mcp_server.py ./
COPY docker-entrypoint.sh ./
RUN uv sync --frozen --no-dev
RUN chmod 0755 /app/docker-entrypoint.sh

USER app

EXPOSE 8000
EXPOSE 9222

CMD ["./docker-entrypoint.sh"]
