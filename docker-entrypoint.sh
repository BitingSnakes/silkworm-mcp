#!/bin/sh
set -eu

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

trap cleanup INT TERM

lightpanda serve --log_format pretty --log_level info --host 127.0.0.1 --port 9222 &
lightpanda_pid=$!

python mcp_server.py --transport http --host 0.0.0.0 --port 8000 &
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
