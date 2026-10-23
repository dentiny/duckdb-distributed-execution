#!/usr/bin/env bash
# Generate TPC-H data into a local .duckdb file, then copy it into the ObjFS database `tpch` at DATA_PATH. Skips the
# copy when that database already has tables.
# Usage: load.sh <sf> <s3://bucket/root>   (source rustfs.env first)
# Run it before starting the driver: ObjFS allows one writer, and the driver opens the database read-write.
set -euo pipefail
SF=$1
DATA_PATH=$2
DUCKDB=${DUCKDB:-$(cd "$(dirname "$0")/../.." && pwd)/build/release/duckdb}
TPCH_FILE=${TPCH_FILE:-tpch_sf$SF.duckdb}

path=${DATA_PATH#s3://}
bucket=${path%%/*}
root=${path#"$bucket"}
root=${root#/}

objfs_sql="$S3_SETUP_SQL
LOAD duckdb_object_storage;
SET duckdb_objfs_backend = 's3';
SET duckdb_objfs_bucket = '$bucket';
SET duckdb_objfs_root = '$root';
ATTACH 'duckdb_objfs://tpch' AS objdb;"

# Skip the copy when the database already has tables, e.g. from an earlier run. The count is the last line printed.
tables=$("$DUCKDB" -bail -noheader -list <<<"$objfs_sql
SELECT count(*) FROM duckdb_tables() WHERE database_name = 'objdb';" | tail -n 1)
if ((tables > 0)); then
	echo "$DATA_PATH already has $tables tables; skipping the load"
	exit 0
fi

# The local file is kept as the reference for verify.sh.
[[ -f $TPCH_FILE ]] || "$DUCKDB" "$TPCH_FILE" -c "CALL dbgen(sf = $SF);"

# The ObjFS database keeps uncommitted rows in memory until a checkpoint, so copy the rows in batches with a checkpoint
# after each. Batches follow rowid order, so the tables keep the key order dbgen wrote them in.
BATCH_ROWS=${BATCH_ROWS:-10000000}
copy_sql="$objfs_sql
ATTACH '$TPCH_FILE' AS src (READ_ONLY);
COPY FROM DATABASE src TO objdb (SCHEMA);"
while IFS='|' read -r table rows; do
	for ((start = 0; start < rows; start += BATCH_ROWS)); do
		copy_sql+="
INSERT INTO objdb.$table SELECT * FROM src.$table WHERE rowid >= $start AND rowid < $((start + BATCH_ROWS));
CHECKPOINT objdb;"
	done
	copy_sql+="
SELECT CASE WHEN (SELECT count(*) FROM objdb.$table) = (SELECT count(*) FROM src.$table) THEN true
	ELSE error('Copied a different number of rows of $table') END AS copied_$table;"
done < <("$DUCKDB" -readonly -noheader -list "$TPCH_FILE" -c "SELECT table_name, estimated_size FROM duckdb_tables()")

"$DUCKDB" -bail <<SQL
$copy_sql
SELECT table_name, estimated_size FROM duckdb_tables() WHERE database_name = 'objdb' ORDER BY table_name;
SQL
