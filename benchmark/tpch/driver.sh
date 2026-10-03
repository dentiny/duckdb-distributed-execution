#!/usr/bin/env bash
# Start the driver inside a DuckDB session and register remote workers; runs until killed.
# Usage: driver.sh <port> [worker_host:port ...]
# Example: driver.sh 8815 10.0.0.2:8816 10.0.0.3:8816
# The `distributed_server` executable cannot register remote workers, so the driver is started from SQL.
# Optional environment: READY_FILE receives the registered worker count once the driver is ready; PID_FILE receives
# the DuckDB process ID, and `kill $(cat PID_FILE)` stops the driver.
set -euo pipefail
PORT=$1
shift
DUCKDB=${DUCKDB:-$(cd "$(dirname "$0")/../.." && pwd)/build/release/duckdb}

{
	echo "SELECT duckherder_start_local_server($PORT, 0);"
	idx=0
	for worker in "$@"; do
		idx=$((idx + 1))
		echo "SELECT duckherder_register_worker('w$idx', 'grpc://$worker');"
	done
	echo "SELECT duckherder_get_worker_count() AS registered_workers;"
	# Signal readiness through a file DuckDB writes itself; redirected stdout may stay buffered.
	if [[ -n ${READY_FILE:-} ]]; then
		echo "COPY (SELECT duckherder_get_worker_count()) TO '$READY_FILE' (FORMAT csv, HEADER false);"
	fi
	# Keep stdin open so the session, and the driver inside it, stays alive. Writing a blank line every second makes
	# this loop exit on SIGPIPE once DuckDB is gone.
	while sleep 1; do echo; done
} | "$DUCKDB" -bail &
if [[ -n ${PID_FILE:-} ]]; then
	echo $! >"$PID_FILE"
fi
wait
