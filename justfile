set shell := ["bash", "-eu", "-o", "pipefail", "-c"]

default:
    @just --list

sync:
    uv sync --group dev

run transport="stdio":
    uv run python mcp_server.py --transport {{transport}}

run-stdio:
    uv run python mcp_server.py --transport stdio

run-http host="127.0.0.1" port="8000":
    uv run python mcp_server.py --transport http --host {{host}} --port {{port}}

lint:
    uv run --group dev ruff check .

format:
    uv run --group dev ruff format .

test:
    uv run --group dev pytest

test-e2e:
    uv run --group dev pytest tests/test_e2e.py

test-file path:
    uv run --group dev pytest {{path}}

smoke:
    if [[ -f mcp_test.py ]]; then \
      uv run python mcp_test.py; \
    else \
      uv run python tests/test_mcp.py; \
    fi

check:
    uv run --group dev ruff check .
    uv run --group dev pytest

docker-build tag="silkworm-mcp":
    docker build -t {{tag}} .

docker-run port="8000" tag="silkworm-mcp":
    docker run --rm -it -p {{port}}:8000 {{tag}}

compose-up:
    docker compose up --build

compose-down:
    docker compose down

compose-logs:
    docker compose logs -f silkworm-mcp
