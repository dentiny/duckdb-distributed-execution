#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RUSTFS_IMAGE="${RUSTFS_IMAGE:-rustfs/rustfs:latest}"
RUSTFS_CONTAINER="${RUSTFS_CONTAINER:-duckherder-rustfs}"
RUSTFS_NETWORK="${RUSTFS_NETWORK:-duckherder-rustfs}"
RUSTFS_VOLUME="${RUSTFS_VOLUME:-duckherder-rustfs-data}"
RUSTFS_HOST="${RUSTFS_HOST:-127.0.0.1}"
RUSTFS_S3_PORT="${RUSTFS_S3_PORT:-19000}"
RUSTFS_CONSOLE_PORT="${RUSTFS_CONSOLE_PORT:-19001}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:-rustfsadmin}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:-rustfsadmin}"
RUSTFS_BUCKET="${RUSTFS_BUCKET:-duckherder}"
RUSTFS_ROOT="${RUSTFS_ROOT:-duckherder-test}"
DUCKDB_BIN="${DUCKDB_BIN:-${ROOT_DIR}/build/reldebug/duckdb}"

usage() {
	cat <<EOF
Usage: $(basename "$0") <command>

Commands:
  start    Start RustFS and create the test bucket
  stop     Stop RustFS (persistent data is retained)
  restart  Restart RustFS
  status   Show service status and connection settings
  logs     Follow RustFS logs
  test     Smoke-test duckdb_object_storage write and read
  reset    Stop RustFS and delete all persisted test data

Configuration can be overridden with RUSTFS_* environment variables.
EOF
}

require_command() {
	if ! command -v "$1" >/dev/null 2>&1; then
		echo "Required command not found: $1" >&2
		exit 1
	fi
}

container_exists() {
	docker container inspect "${RUSTFS_CONTAINER}" >/dev/null 2>&1
}

container_running() {
	[[ "$(docker container inspect --format '{{.State.Running}}' "${RUSTFS_CONTAINER}" 2>/dev/null || true)" == "true" ]]
}

wait_until_ready() {
	local deadline=$((SECONDS + 90))

	printf "Waiting for RustFS"
	until curl --fail --silent "http://${RUSTFS_HOST}:${RUSTFS_S3_PORT}/health" >/dev/null 2>&1; do
		if ! container_running; then
			echo
			echo "RustFS exited before becoming ready:" >&2
			docker logs "${RUSTFS_CONTAINER}" >&2 || true
			return 1
		fi
		if ((SECONDS >= deadline)); then
			echo
			echo "Timed out waiting for RustFS health endpoint" >&2
			docker logs "${RUSTFS_CONTAINER}" >&2 || true
			return 1
		fi
		printf "."
		sleep 1
	done
	echo " ready"
}

create_bucket() {
	# A signed S3 PUT creates the bucket; an existing bucket returns 200 or 409 depending on the server. --aws-sigv4
	# needs curl 7.75 or later. On failure curl still prints 000, so keep going to report it.
	local status
	status=$(curl --silent --output /dev/null --write-out '%{http_code}' \
		--aws-sigv4 "aws:amz:us-east-1:s3" --user "${RUSTFS_ACCESS_KEY}:${RUSTFS_SECRET_KEY}" \
		-X PUT "http://${RUSTFS_HOST}:${RUSTFS_S3_PORT}/${RUSTFS_BUCKET}") || true
	if [[ "${status}" != 200 && "${status}" != 409 ]]; then
		echo "Failed to create bucket '${RUSTFS_BUCKET}' (HTTP ${status}; 000 means curl failed, and it needs 7.75 or later)" >&2
		return 1
	fi
}

print_connection_info() {
	cat <<EOF
S3 endpoint:  http://${RUSTFS_HOST}:${RUSTFS_S3_PORT}
Console:      http://${RUSTFS_HOST}:${RUSTFS_CONSOLE_PORT}
Bucket:       ${RUSTFS_BUCKET}
Root prefix:  ${RUSTFS_ROOT}
Access key:   ${RUSTFS_ACCESS_KEY}
Secret key:   ${RUSTFS_SECRET_KEY}

DuckDB S3 secret:
CREATE OR REPLACE SECRET rustfs (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID '${RUSTFS_ACCESS_KEY}',
    SECRET '${RUSTFS_SECRET_KEY}',
    REGION 'us-east-1',
    ENDPOINT '${RUSTFS_HOST}:${RUSTFS_S3_PORT}',
    USE_SSL false,
    URL_STYLE 'path',
    SCOPE 's3://${RUSTFS_BUCKET}/${RUSTFS_ROOT}'
);
EOF
}

start_service() {
	require_command docker
	require_command curl

	if container_running; then
		echo "RustFS is already running."
		create_bucket
		print_connection_info
		return
	fi

	docker network inspect "${RUSTFS_NETWORK}" >/dev/null 2>&1 ||
		docker network create "${RUSTFS_NETWORK}" >/dev/null
	docker volume inspect "${RUSTFS_VOLUME}" >/dev/null 2>&1 ||
		docker volume create "${RUSTFS_VOLUME}" >/dev/null

	if container_exists; then
		docker rm "${RUSTFS_CONTAINER}" >/dev/null
	fi

	docker run --detach \
		--name "${RUSTFS_CONTAINER}" \
		--network "${RUSTFS_NETWORK}" \
		--restart unless-stopped \
		-p "${RUSTFS_HOST}:${RUSTFS_S3_PORT}:9000" \
		-p "${RUSTFS_HOST}:${RUSTFS_CONSOLE_PORT}:9001" \
		-e "RUSTFS_VOLUMES=/data" \
		-e "RUSTFS_ADDRESS=0.0.0.0:9000" \
		-e "RUSTFS_CONSOLE_ADDRESS=0.0.0.0:9001" \
		-e "RUSTFS_CONSOLE_ENABLE=true" \
		-e "RUSTFS_ACCESS_KEY=${RUSTFS_ACCESS_KEY}" \
		-e "RUSTFS_SECRET_KEY=${RUSTFS_SECRET_KEY}" \
		-v "${RUSTFS_VOLUME}:/data" \
		"${RUSTFS_IMAGE}" >/dev/null

	wait_until_ready
	create_bucket
	echo "Bucket '${RUSTFS_BUCKET}' is ready."
	print_connection_info
}

stop_service() {
	require_command docker
	if container_exists; then
		docker rm --force "${RUSTFS_CONTAINER}" >/dev/null
		echo "RustFS stopped. Volume '${RUSTFS_VOLUME}' was retained."
	else
		echo "RustFS is not running."
	fi
}

show_status() {
	require_command docker
	if container_running; then
		echo "RustFS is running."
		print_connection_info
	else
		echo "RustFS is stopped."
		return 1
	fi
}

follow_logs() {
	require_command docker
	if ! container_exists; then
		echo "RustFS container does not exist; run '$(basename "$0") start' first." >&2
		exit 1
	fi
	docker logs --follow "${RUSTFS_CONTAINER}"
}

smoke_test() {
	start_service

	if [[ ! -x "${DUCKDB_BIN}" ]]; then
		echo "DuckDB executable not found at ${DUCKDB_BIN}; run 'make reldebug' or set DUCKDB_BIN." >&2
		exit 1
	fi

	local work_dir
	local test_root
	work_dir="$(mktemp -d)"
	test_root="${RUSTFS_ROOT}/smoke-$RANDOM-$$"
	trap 'rm -rf "${work_dir}"' RETURN

	"${DUCKDB_BIN}" -bail <<SQL
SET extension_directory = '${work_dir}/extensions';
FORCE INSTALL cache_httpfs FROM community;
LOAD cache_httpfs;
LOAD duckdb_object_storage;
CREATE SECRET rustfs (
    TYPE S3,
    PROVIDER CONFIG,
    KEY_ID '${RUSTFS_ACCESS_KEY}',
    SECRET '${RUSTFS_SECRET_KEY}',
    REGION 'us-east-1',
    ENDPOINT '${RUSTFS_HOST}:${RUSTFS_S3_PORT}',
    USE_SSL false,
    URL_STYLE 'path',
    SCOPE 's3://${RUSTFS_BUCKET}/${RUSTFS_ROOT}'
);
SET duckdb_objfs_backend = 's3';
SET duckdb_objfs_bucket = '${RUSTFS_BUCKET}';
SET duckdb_objfs_root = '${test_root}';
ATTACH 'duckdb_objfs://smoke.db' AS object_db;
CREATE TABLE object_db.items(id INTEGER, value VARCHAR);
INSERT INTO object_db.items VALUES (1, 'written to RustFS'), (2, 'read from RustFS');
CHECKPOINT object_db;
DETACH object_db;
ATTACH 'duckdb_objfs://smoke.db' AS object_db (READ_ONLY);
SELECT CASE
    WHEN count(*) = 2 AND sum(id) = 3 THEN 'RustFS extension smoke test passed'
    ELSE error('Unexpected data read from RustFS')
END AS result
FROM object_db.items;
SQL

	rm -rf "${work_dir}"
	trap - RETURN
}

reset_service() {
	stop_service
	docker volume rm "${RUSTFS_VOLUME}" >/dev/null 2>&1 || true
	docker network rm "${RUSTFS_NETWORK}" >/dev/null 2>&1 || true
	echo "Deleted RustFS test data."
}

case "${1:-}" in
start)
	start_service
	;;
stop)
	stop_service
	;;
restart)
	stop_service
	start_service
	;;
status)
	show_status
	;;
logs)
	follow_logs
	;;
test)
	smoke_test
	;;
reset)
	reset_service
	;;
*)
	usage
	exit 1
	;;
esac
