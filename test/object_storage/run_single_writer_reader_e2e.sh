#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/reldebug/duckdb}"
MINIO_PORT="${DUCKDB_OBJFS_MINIO_PORT:-19000}"
S3_BUCKET="${DUCKDB_OBJFS_S3_BUCKET:-duckdb-objfs-writer-reader}"
S3_ROOT="${DUCKDB_OBJFS_S3_ROOT:-writer-reader-$$}"
MINIO_IMAGE="${DUCKDB_OBJFS_MINIO_IMAGE:-quay.io/minio/minio}"
MC_IMAGE="${DUCKDB_OBJFS_MC_IMAGE:-quay.io/minio/mc}"
MINIO_USER="${DUCKDB_OBJFS_MINIO_USER:-minioadmin}"
MINIO_PASSWORD="${DUCKDB_OBJFS_MINIO_PASSWORD:-minioadmin}"
CONTAINER_NAME="duckdb-objfs-writer-reader-$$"
WORK_DIR="$(mktemp -d)"
WRITER_PID=""
READER_PID=""

cleanup() {
	if [[ -n "${WRITER_PID}" ]]; then
		kill "${WRITER_PID}" >/dev/null 2>&1 || true
	fi
	if [[ -n "${READER_PID}" ]]; then
		kill "${READER_PID}" >/dev/null 2>&1 || true
	fi
	docker rm -f "${CONTAINER_NAME}" >/dev/null 2>&1 || true
	rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

wait_for_output() {
	local pattern="$1"
	local output_file="$2"
	local process_id="$3"
	local deadline=$((SECONDS + 60))

	while ! grep -qF "${pattern}" "${output_file}" 2>/dev/null; do
		if ! kill -0 "${process_id}" 2>/dev/null; then
			cat "${output_file}" >&2
			return 1
		fi
		if ((SECONDS >= deadline)); then
			echo "Timed out waiting for '${pattern}'" >&2
			cat "${output_file}" >&2
			return 1
		fi
		sleep 0.2
	done
}

send_connection_setup() {
	local descriptor="$1"

	cat >&"${descriptor}" <<SQL
SET extension_directory = '${WORK_DIR}/extensions';
LOAD cache_httpfs;
LOAD duckdb_object_storage;
CREATE OR REPLACE SECRET duckdb_objfs_s3 (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID '${MINIO_USER}',
    SECRET '${MINIO_PASSWORD}',
    REGION 'us-east-1',
    ENDPOINT '127.0.0.1:${MINIO_PORT}',
    USE_SSL false,
    URL_STYLE 'path',
    SCOPE 's3://${S3_BUCKET}'
);
SET duckdb_objfs_backend = 's3';
SET duckdb_objfs_bucket = '${S3_BUCKET}';
SET duckdb_objfs_root = '${S3_ROOT}';
SQL
}

if [[ ! -x "${DUCKDB_BIN}" ]]; then
	echo "DuckDB executable not found at ${DUCKDB_BIN}; run 'make reldebug' first" >&2
	exit 1
fi

docker run --detach --rm --name "${CONTAINER_NAME}" -p "${MINIO_PORT}:9000" \
	-e "MINIO_ROOT_USER=${MINIO_USER}" \
	-e "MINIO_ROOT_PASSWORD=${MINIO_PASSWORD}" \
	"${MINIO_IMAGE}" server /data >/dev/null

until curl --fail --silent "http://127.0.0.1:${MINIO_PORT}/minio/health/ready" >/dev/null; do
	sleep 0.2
done

docker run --rm --network "container:${CONTAINER_NAME}" \
	-e "MC_HOST_local=http://${MINIO_USER}:${MINIO_PASSWORD}@127.0.0.1:9000" \
	"${MC_IMAGE}" \
	mb --ignore-existing "local/${S3_BUCKET}" >/dev/null

# Install the S3 secret provider once before the two processes share it.
"${DUCKDB_BIN}" -bail <<SQL
SET extension_directory = '${WORK_DIR}/extensions';
FORCE INSTALL cache_httpfs FROM community;
SQL

mkfifo "${WORK_DIR}/writer.in" "${WORK_DIR}/reader.in"
touch "${WORK_DIR}/writer.out" "${WORK_DIR}/reader.out"
exec 3<>"${WORK_DIR}/writer.in"
exec 4<>"${WORK_DIR}/reader.in"

"${DUCKDB_BIN}" -bail -csv -noheader \
	<"${WORK_DIR}/writer.in" >"${WORK_DIR}/writer.out" 2>&1 &
WRITER_PID="$!"

send_connection_setup 3
cat >&3 <<'SQL'
ATTACH 'duckdb_objfs://shared.db' AS object_db;
CREATE TABLE object_db.items(i INTEGER);
INSERT INTO object_db.items VALUES (1), (2);
CHECKPOINT object_db;
SELECT 'WRITER_READY';
SQL
wait_for_output "WRITER_READY" "${WORK_DIR}/writer.out" "${WRITER_PID}"

"${DUCKDB_BIN}" -bail -csv -noheader \
	<"${WORK_DIR}/reader.in" >"${WORK_DIR}/reader.out" 2>&1 &
READER_PID="$!"

send_connection_setup 4
cat >&4 <<'SQL'
ATTACH 'duckdb_objfs://shared.db' AS object_db (READ_ONLY);
SELECT 'READER_SUM=' || sum(i)::VARCHAR FROM object_db.items;
SQL
wait_for_output "READER_SUM=3" "${WORK_DIR}/reader.out" "${READER_PID}"

# The reader stays attached while the sole writer commits another checkpoint.
cat >&3 <<'SQL'
INSERT INTO object_db.items VALUES (3);
CHECKPOINT object_db;
SELECT 'WRITER_SUM=' || sum(i)::VARCHAR FROM object_db.items;
SQL
wait_for_output "WRITER_SUM=6" "${WORK_DIR}/writer.out" "${WRITER_PID}"

printf '.quit\n' >&4
printf '.quit\n' >&3
wait "${READER_PID}"
READER_PID=""
wait "${WRITER_PID}"
WRITER_PID=""

echo "Single writer/read-only reader S3 test passed"
