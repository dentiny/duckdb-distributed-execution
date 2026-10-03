#!/usr/bin/env bash
# Generate TPC-H data into a local .duckdb file, then copy it into the ObjFS database `tpch` at DATA_PATH.
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

# The local file is kept as the reference for verify.sh.
[[ -f $TPCH_FILE ]] || "$DUCKDB" "$TPCH_FILE" -c "CALL dbgen(sf = $SF);"

"$DUCKDB" -bail <<SQL
$S3_SETUP_SQL
LOAD duckdb_object_storage;
ATTACH '$TPCH_FILE' AS src (READ_ONLY);
SET duckdb_objfs_backend = 's3';
SET duckdb_objfs_bucket = '$bucket';
SET duckdb_objfs_root = '$root';
ATTACH 'duckdb_objfs://tpch' AS objdb;
COPY FROM DATABASE src TO objdb;
CHECKPOINT objdb;
SELECT table_name, estimated_size FROM duckdb_tables() WHERE database_name = 'objdb' ORDER BY table_name;
SQL
