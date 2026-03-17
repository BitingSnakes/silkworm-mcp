#!/bin/sh
set -eu

is_truthy() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

: "${MCP_TRANSPORT:=http}"
: "${MCP_HOST:=0.0.0.0}"
: "${MCP_PORT:=8000}"
: "${MCP_PATH:=}"
: "${LIGHTPANDA_ENABLED:=1}"
: "${LIGHTPANDA_HOST:=127.0.0.1}"
: "${LIGHTPANDA_PORT:=9222}"
: "${LIGHTPANDA_LOG_FORMAT:=pretty}"
: "${LIGHTPANDA_LOG_LEVEL:=info}"

if [ -z "${SILKWORM_MCP_READINESS_CDP_WS_ENDPOINT:-}" ]; then
  export SILKWORM_MCP_READINESS_CDP_WS_ENDPOINT="ws://${LIGHTPANDA_HOST}:${LIGHTPANDA_PORT}"
fi

if [ -z "${SILKWORM_MCP_READINESS_REQUIRE_CDP:-}" ]; then
  if is_truthy "$LIGHTPANDA_ENABLED"; then
    export SILKWORM_MCP_READINESS_REQUIRE_CDP=true
  else
    export SILKWORM_MCP_READINESS_REQUIRE_CDP=false
  fi
fi

set -- python mcp_server.py --transport "$MCP_TRANSPORT" --host "$MCP_HOST" --port "$MCP_PORT"
if [ -n "$MCP_PATH" ]; then
  set -- "$@" --path "$MCP_PATH"
fi

lightpanda_pid=''
mcp_pid=''

cleanup() {
  if [ -n "$mcp_pid" ] && kill -0 "$mcp_pid" 2>/dev/null; then
    kill "$mcp_pid" 2>/dev/null || true
  fi
  if [ -n "$lightpanda_pid" ] && kill -0 "$lightpanda_pid" 2>/dev/null; then
    kill "$lightpanda_pid" 2>/dev/null || true
  fi
}

trap cleanup EXIT INT TERM HUP

if ! is_truthy "$LIGHTPANDA_ENABLED"; then
  exec "$@"
fi

lightpanda serve \
  --log_format "$LIGHTPANDA_LOG_FORMAT" \
  --log_level "$LIGHTPANDA_LOG_LEVEL" \
  --host "$LIGHTPANDA_HOST" \
  --port "$LIGHTPANDA_PORT" &
lightpanda_pid=$!

"$@" &
mcp_pid=$!

exit_code=0

while kill -0 "$lightpanda_pid" 2>/dev/null && kill -0 "$mcp_pid" 2>/dev/null; do
  sleep 1
done

if ! kill -0 "$lightpanda_pid" 2>/dev/null; then
  wait "$lightpanda_pid" || exit_code=$?
fi

if ! kill -0 "$mcp_pid" 2>/dev/null; then
  wait "$mcp_pid" || exit_code=$?
fi

cleanup
wait "$lightpanda_pid" 2>/dev/null || true
wait "$mcp_pid" 2>/dev/null || true

exit "$exit_code"
