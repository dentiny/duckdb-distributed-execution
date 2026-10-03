#!/usr/bin/env bash
# Run the TPC-H benchmark end to end: load the data once, then for each worker count start the driver with that many
# workers, verify results, time the queries, and stop the driver. See README.md for options.
# Results go to results/<time>-sf<N>/: metadata.json, n<N>.csv/.log, verify-n<N>.txt, process logs, summary.csv.
set -euo pipefail

DIR=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$DIR/../.." && pwd)
export DUCKDB=${DUCKDB:-$ROOT/build/release/duckdb}
WORKER_BIN=${WORKER_BIN:-$ROOT/build/release/extension/duckherder/distributed_worker}
DRIVER_PORT=${DRIVER_PORT:-8815}
# DuckDB executable on the --driver-ssh host.
REMOTE_DUCKDB=${REMOTE_DUCKDB:-duckdb}
# Optional cgroup isolation for local processes (Linux, systemd): one CPU list per worker, e.g. "4-7 8-11 12-15", and
# one for the driver, e.g. "8-9". The client stays in whatever cgroup bench.sh runs in.
read -r -a worker_cpus <<<"${WORKER_CPUS:-}"
WORKER_MEMORY=${WORKER_MEMORY:-4G}
DRIVER_CPUS=${DRIVER_CPUS:-}
DRIVER_MEMORY=${DRIVER_MEMORY:-4G}

SF=1
WORKER_COUNTS="0 2"
REPS=5
WARMUP=1
COLD=0
ENV_FILE=$DIR/rustfs.env
DATA_PATH=""
VERIFY=1
LOAD=1
WORKER_HOSTS=""
DRIVER_SSH=""
ENDPOINT=""
while (($#)); do
	case $1 in
	--sf) SF=$2 && shift 2 ;;
	--workers) WORKER_COUNTS=$2 && shift 2 ;;
	--reps) REPS=$2 && shift 2 ;;
	--warmup) WARMUP=$2 && shift 2 ;;
	--cold) COLD=1 && shift ;;
	--env) ENV_FILE=$2 && shift 2 ;;
	--data-path) DATA_PATH=$2 && shift 2 ;;
	--skip-verify) VERIFY=0 && shift ;;
	--no-load) LOAD=0 && shift ;;
	--worker-hosts) WORKER_HOSTS=$2 && shift 2 ;;
	--driver-ssh) DRIVER_SSH=$2 && shift 2 ;;
	--driver-endpoint) ENDPOINT=$2 && shift 2 ;;
	*) echo "Unknown option: $1" >&2 && exit 1 ;;
	esac
done
DATA_PATH=${DATA_PATH:-s3://duckherder/tpch-sf$SF}
ENDPOINT=${ENDPOINT:-localhost:$DRIVER_PORT}
# Cold runs restart every process before each query, so there is nothing to warm up.
((COLD)) && WARMUP=0
# shellcheck source=/dev/null
source "$ENV_FILE"

WORK=$DIR/work
OUT=$DIR/results/$(date +%Y%m%d-%H%M%S)-sf$SF
mkdir -p "$WORK" "$OUT"
export TPCH_FILE=$WORK/tpch_sf$SF.duckdb

die() {
	echo "$*" >&2
	exit 1
}

port_open() { (echo >"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# Counts the CPUs in a list such as "4-7" or "0,2,4-5".
cpu_count() {
	local count=0 part
	for part in ${1//,/ }; do
		if [[ $part == *-* ]]; then
			count=$((count + ${part#*-} - ${part%-*} + 1))
		else
			count=$((count + 1))
		fi
	done
	echo "$count"
}

# Runs a command limited to the given CPUs and memory through a systemd scope, or plainly without CPUs. Only call it
# in the background: it replaces the current shell so that $! is the process itself.
launch_limited() {
	local cpus=$1 memory=$2
	shift 2
	if [[ -z $cpus ]]; then
		exec "$@"
	fi
	# The CPU quota also tells DuckDB how many threads to start; it ignores the cpuset.
	exec systemd-run --user --scope --quiet -p "AllowedCPUs=$cpus" -p "CPUQuota=$(($(cpu_count "$cpus") * 100))%" \
		-p "MemoryMax=$memory" "$@"
}

# Multiplex SSH so polling the remote driver is cheap. The socket path must stay short.
SSH_OPTS=(-o BatchMode=yes -o ControlMaster=auto -o "ControlPath=/tmp/dh-ssh-%C" -o ControlPersist=120)
remote() { ssh "${SSH_OPTS[@]}" "$DRIVER_SSH" "$@"; }
REMOTE_STATE=/tmp/duckherder-driver-$DRIVER_PORT

worker_pids=()
driver_pid=""
driver_pidfile=""
driver_readyfile=""

start_driver() {
	local log=$1
	shift
	if [[ -z $DRIVER_SSH ]]; then
		driver_pidfile=${log%.log}.pid
		driver_readyfile=${log%.log}.ready
		# Restarts reuse these paths; stale files would look like the new driver's.
		rm -f "$driver_readyfile" "$driver_pidfile"
		launch_limited "$DRIVER_CPUS" "$DRIVER_MEMORY" env READY_FILE="$driver_readyfile" PID_FILE="$driver_pidfile" \
			"$DIR/driver.sh" "$DRIVER_PORT" "$@" >"$log" 2>&1 &
	else
		remote "rm -f $REMOTE_STATE.ready $REMOTE_STATE.pid"
		remote "DUCKDB='$REMOTE_DUCKDB' READY_FILE=$REMOTE_STATE.ready PID_FILE=$REMOTE_STATE.pid bash -s -- $DRIVER_PORT $*" \
			<"$DIR/driver.sh" >"$log" 2>&1 &
	fi
	driver_pid=$!
}

# Prints the registered worker count once the driver is ready, or nothing before that.
driver_ready() {
	if [[ -z $DRIVER_SSH ]]; then
		cat "$driver_readyfile" 2>/dev/null || true
	else
		remote "cat $REMOTE_STATE.ready 2>/dev/null" || true
	fi
}

stop_driver() {
	[[ -n $driver_pid ]] || return 0
	if [[ -z $DRIVER_SSH ]]; then
		[[ -f $driver_pidfile ]] && kill "$(cat "$driver_pidfile")" 2>/dev/null
	else
		remote "kill \$(cat $REMOTE_STATE.pid)" 2>/dev/null
	fi || kill "$driver_pid" 2>/dev/null || true
	wait "$driver_pid" 2>/dev/null || true
	driver_pid=""
}

stop_all() {
	stop_driver
	# Workers attach read-only and keep no state. SIGTERM makes them abort in their signal handler, so skip it.
	for pid in ${worker_pids[@]+"${worker_pids[@]}"}; do
		kill -KILL "$pid" 2>/dev/null || true
		wait "$pid" 2>/dev/null || true
	done
	worker_pids=()
}
trap stop_all EXIT

# Leftover local processes would answer on these ports and silently replace the ones started here.
max_workers=0
for n in $WORKER_COUNTS; do ((n > max_workers)) && max_workers=$n; done
local_ports=()
[[ -n $DRIVER_SSH ]] || local_ports+=("$DRIVER_PORT")
if [[ -z $WORKER_HOSTS ]]; then
	for ((port = DRIVER_PORT + 1; port <= DRIVER_PORT + max_workers; port++)); do local_ports+=("$port"); done
	((${#worker_cpus[@]} == 0 || max_workers <= ${#worker_cpus[@]})) ||
		die "--workers needs $max_workers CPU lists in WORKER_CPUS"
else
	read -r -a remote_workers <<<"$WORKER_HOSTS"
	((max_workers <= ${#remote_workers[@]})) || die "--workers needs $max_workers hosts in --worker-hosts"
fi
for port in ${local_ports[@]+"${local_ports[@]}"}; do
	port_open "$port" && die "Port $port is in use; stop the running driver or worker first."
done

# Load once per data path. ObjFS allows one writer, and the driver opens the database read-write, so load first.
marker=$WORK/.loaded-$(echo "$DATA_PATH" | tr -c 'a-zA-Z0-9\n' _)
if ((LOAD)) && [[ ! -f $marker ]]; then
	"$DIR/load.sh" "$SF" "$DATA_PATH" | tee "$OUT/load.log"
	touch "$marker"
fi
# verify.sh compares against this file; with --no-load it may not exist yet.
if ((VERIFY)) && [[ ! -f $TPCH_FILE ]]; then
	"$DUCKDB" "$TPCH_FILE" -c "CALL dbgen(sf = $SF);" >/dev/null
fi

# DUCKHERDER_STARTUP_SQL as a JSON string.
startup_sql_json=$(printf '%s' "${DUCKHERDER_STARTUP_SQL:-}" | python3 -c 'import json, sys; print(json.dumps(sys.stdin.read()))')
cat >"$OUT/metadata.json" <<EOF
{
  "commit": "$(git -C "$ROOT" rev-parse HEAD)",
  "modified_tracked_files": $(git -C "$ROOT" status --porcelain --untracked-files=no | wc -l | tr -d ' '),
  "duckdb": "$("$DUCKDB" -noheader -list -c 'SELECT version()')",
  "sf": $SF,
  "worker_counts": "$WORKER_COUNTS",
  "reps": $REPS,
  "warmup": $WARMUP,
  "cold": $COLD,
  "worker_cpus": "${WORKER_CPUS:-}",
  "worker_memory": "$WORKER_MEMORY",
  "driver_cpus": "$DRIVER_CPUS",
  "driver_memory": "$DRIVER_MEMORY",
  "startup_sql": $startup_sql_json,
  "data_path": "$DATA_PATH",
  "driver_endpoint": "$ENDPOINT",
  "driver_ssh": "$DRIVER_SSH",
  "worker_hosts": "$WORKER_HOSTS",
  "client_host": "$(uname -sm)",
  "client_cpus": $(getconf _NPROCESSORS_ONLN),
  "started_at": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

# Starts the driver with n workers (local workers are started too) and waits until all are registered.
start_cluster() {
	local n=$1 idx port registered deadline workers=()
	if [[ -n $WORKER_HOSTS ]]; then
		workers=(${remote_workers[@]+"${remote_workers[@]:0:n}"})
	else
		for ((idx = 1; idx <= n; idx++)); do
			port=$((DRIVER_PORT + idx))
			launch_limited "${worker_cpus[idx - 1]:-}" "$WORKER_MEMORY" "$WORKER_BIN" 127.0.0.1 "$port" "w$idx" \
				>"$OUT/n$n-worker$idx.log" 2>&1 &
			worker_pids+=($!)
			workers+=("127.0.0.1:$port")
			deadline=$((SECONDS + 30))
			until port_open "$port"; do
				((SECONDS < deadline)) || die "Timed out waiting for worker on port $port (see $OUT/n$n-worker$idx.log)"
				sleep 0.2
			done
		done
	fi

	start_driver "$OUT/n$n-driver.log" ${workers[@]+"${workers[@]}"}
	deadline=$((SECONDS + 60))
	until registered=$(driver_ready) && [[ -n $registered ]]; do
		kill -0 "$driver_pid" 2>/dev/null || die "Driver exited during startup (see $OUT/n$n-driver.log)"
		((SECONDS < deadline)) || die "Timed out waiting for the driver (see $OUT/n$n-driver.log)"
		sleep 0.5
	done
	registered=$(echo "$registered" | tr -d '[:space:]')
	[[ $registered == "$n" ]] || die "Driver registered $registered workers, expected $n"
}

failed=()
for n in $WORKER_COUNTS; do
	echo "== $n workers"
	start_cluster "$n"
	if ((VERIFY)); then
		mkdir -p "$OUT/verify-n$n"
		if ! OUT=$OUT/verify-n$n "$DIR/verify.sh" "$ENDPOINT" "$DATA_PATH" "$TPCH_FILE" | tee "$OUT/verify-n$n.txt"; then
			failed+=("verify with $n workers")
		fi
	fi
	if ((COLD)); then
		# Restart every process before each run, so that no run reads data cached by an earlier one.
		stop_all
		mkdir -p "$OUT/cold-n$n"
		echo "query,run,seconds" >"$OUT/n$n.csv"
		for q in $(seq 1 22); do
			printf 'Q%s ' "$q"
			for ((r = 0; r < REPS; r++)); do
				start_cluster "$n"
				QUERIES=$q WARMUP=0 REPS=1 "$DIR/run.sh" "$OUT/cold-n$n/q$q-r$r" "$ENDPOINT" "$DATA_PATH" >/dev/null
				awk -F, -v r="$r" 'NR > 1 { print $1 "," r "," $3 }' "$OUT/cold-n$n/q$q-r$r.csv" >>"$OUT/n$n.csv"
				stop_all
			done
		done
		echo
		echo "Wrote $OUT/n$n.csv; per-run logs in $OUT/cold-n$n"
	else
		WARMUP=$WARMUP REPS=$REPS "$DIR/run.sh" "$OUT/n$n" "$ENDPOINT" "$DATA_PATH"
		stop_all
	fi
done

# Median seconds per query (columns are worker counts), excluding warm-up runs.
"$DUCKDB" -c "
CREATE TABLE timings AS
SELECT regexp_extract(filename, 'n([0-9]+)\.csv', 1)::INTEGER AS workers, query, seconds
FROM read_csv('$OUT/n*.csv', filename = true) WHERE run >= $WARMUP;
COPY (PIVOT timings ON workers USING median(seconds) GROUP BY query ORDER BY query) TO '$OUT/summary.csv';
FROM read_csv('$OUT/summary.csv');"

echo "Results in $OUT"
if ((${#failed[@]})); then
	printf 'FAILED: %s\n' "${failed[@]}" >&2
	exit 1
fi
