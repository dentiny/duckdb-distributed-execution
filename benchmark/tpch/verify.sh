#!/usr/bin/env bash
# Check that the 22 TPC-H queries through duckherder match plain DuckDB on the file load.sh generated.
# Usage: verify.sh <driver_host:port> <s3://bucket/root> <tpch_file>   (source rustfs.env first)
set -euo pipefail
ENDPOINT=$1
DATA_PATH=$2
TPCH_FILE=$3
DIR=$(cd "$(dirname "$0")" && pwd)
DUCKDB=${DUCKDB:-$DIR/../../build/release/duckdb}
OUT=${OUT:-$(mktemp -d)}

queries() {
	for q in $(seq 1 22); do
		echo ".output $OUT/$1_q$q.csv"
		echo "PRAGMA tpch($q);"
	done
}

{
	echo "$S3_SETUP_SQL"
	echo "ATTACH '$ENDPOINT/tpch' AS dh (TYPE duckherder, READ_ONLY, DATA_PATH '$DATA_PATH', SECRET s3);"
	echo "USE dh;"
	echo ".mode csv"
	queries dh
} | "$DUCKDB" -bail >/dev/null # Query results go to the .csv files; errors still reach stderr.

{
	echo "ATTACH '$TPCH_FILE' AS src (READ_ONLY);"
	echo "USE src;"
	echo ".mode csv"
	queries ref
} | "$DUCKDB" -bail

python3 "$DIR/compare.py" "$OUT"
