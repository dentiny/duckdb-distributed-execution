#!/usr/bin/env bash
# Time TPC-H queries through a duckherder driver.
# Usage: run.sh <label> <driver_host:port> <s3://bucket/root>   (source rustfs.env first)
# Writes <label>.log and <label>.csv (query,run,seconds). Each query runs WARMUP (default 1) warm-up times, numbered
# from 0, then REPS (default 5) measured times. QUERIES selects the queries (default 1 to 22).
set -euo pipefail
LABEL=$1
ENDPOINT=$2
DATA_PATH=$3
REPS=${REPS:-5}
WARMUP=${WARMUP:-1}
QUERIES=${QUERIES:-$(seq 1 22)}
DUCKDB=${DUCKDB:-$(cd "$(dirname "$0")/../.." && pwd)/build/release/duckdb}

{
	echo "$S3_SETUP_SQL"
	echo "ATTACH '$ENDPOINT/tpch' AS dh (TYPE duckherder, READ_ONLY, DATA_PATH '$DATA_PATH', SECRET s3);"
	echo "USE dh;"
	# Discard results without printing them; the client still fetches every row.
	echo ".mode trash"
	echo ".timer on"
	for q in $QUERIES; do
		for ((r = 0; r < WARMUP + REPS; r++)); do
			echo ".print q=$q run=$r"
			echo "PRAGMA tpch($q);"
		done
	done
	echo ".timer off"
	echo ".mode csv"
	echo "SELECT * FROM duckherder_get_query_execution_stats();"
} | "$DUCKDB" -bail >"$LABEL.log"

echo "query,run,seconds" >"$LABEL.csv"
awk '/^q=/ { split($1, q, "="); split($2, r, "=") } /^Run Time/ { print q[2] "," r[2] "," $5 }' "$LABEL.log" >>"$LABEL.csv"
echo "Wrote $LABEL.csv ($(($(wc -l <"$LABEL.csv") - 1)) timings) and $LABEL.log"
