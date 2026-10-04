#!/usr/bin/env bash
# Check that TPC-H queries through duckherder match plain DuckDB on the file load.sh generated.
# Usage: verify.sh <driver_host:port> <s3://bucket/root> <tpch_file>   (source rustfs.env first)
# QUERIES selects the queries (default 1 to 22).
set -euo pipefail
ENDPOINT=$1
DATA_PATH=$2
TPCH_FILE=$3
DIR=$(cd "$(dirname "$0")" && pwd)
DUCKDB=${DUCKDB:-$DIR/../../build/release/duckdb}
OUT=${OUT:-$(mktemp -d)}
QUERIES=${QUERIES:-$(seq 1 22)}

queries() {
	for q in $QUERIES; do
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

python3 "$DIR/compare.py" "$OUT" $QUERIES
