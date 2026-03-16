#!/bin/sh
set -eu

lightpanda --remote-debugging-port=9222 >/tmp/lightpanda.log 2>&1 &

exec python mcp_server.py --transport http --host 0.0.0.0 --port 8000
