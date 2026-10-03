# TPC-H benchmark over ObjFS

Runs the 22 TPC-H queries through a duckherder driver with 0..N workers, checks the results, and times them. Data lives in an ObjFS database on S3-compatible storage (local RustFS by default).

Timings are only meaningful on Linux, where each process runs in its own cgroup.

## Build

Requires `pkg-config`, `flex`, `bison`, `libtool`, `cargo`, vcpkg, and Docker for RustFS. Set `LATENCY_INJECTION_FS_DIR` to a [duckdb-filesystem-latency-injection](https://github.com/dentiny/duckdb-filesystem-latency-injection) checkout to build it in for simulating storage latency; leave it unset otherwise.

```sh
LATENCY_INJECTION_FS_DIR=$HOME/duckdb-filesystem-latency-injection \
VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake CORE_EXTENSIONS='tpch' GEN=ninja \
  CMAKE_BUILD_PARALLEL_LEVEL=$(getconf _NPROCESSORS_ONLN) make release
```

## Run

From the repository root:

```sh
# Storage, pinned away from the benchmark CPUs.
scripts/local-rustfs.sh start
docker update --cpuset-cpus 16-19 --memory 2g --memory-swap 2g duckherder-rustfs

# Data, once per scale factor. The local file is the reference for result checks.
source benchmark/tpch/rustfs.env && mkdir -p benchmark/tpch/work
TPCH_FILE=benchmark/tpch/work/tpch_sf10.duckdb benchmark/tpch/load.sh 10 s3://duckherder/tpch-sf10

# Benchmark with S3-like latency: client on CPUs 0-1, driver on 8-9, workers on 2-3, 4-5, and 6-7, each with 4 GiB.
DUCKHERDER_STARTUP_SQL="$(cat benchmark/tpch/s3_latency.sql)" \
WORKER_CPUS="2-3 4-5 6-7" WORKER_MEMORY=4G DRIVER_CPUS=8-9 DRIVER_MEMORY=4G \
systemd-run --user --scope -p AllowedCPUs=0-1 -p CPUQuota=200% -p MemoryMax=4G \
  benchmark/tpch/bench.sh --sf 10 --workers "0 3" --data-path s3://duckherder/tpch-sf10 --no-load --cold
```

`systemd-run --user` needs the `cpuset` controller delegated to user sessions (`Delegate=cpu cpuset io memory pids` in a `user@.service` drop-in).

## Options

| Option | Default | Meaning |
| --- | --- | --- |
| `--sf` | `1` | TPC-H scale factor |
| `--workers` | `"0 2"` | Worker counts to run; with 0 the driver runs queries itself |
| `--reps` | `5` | Measured runs per query |
| `--warmup` | `1` | Warm-up runs per query, excluded from `summary.csv` |
| `--cold` | off | Restart the driver and workers before every run, so no run reuses data cached by an earlier one; no warm-up |
| `--data-path` | `s3://duckherder/tpch-sf<sf>` | Where the ObjFS database lives; loaded on first use unless `--no-load` |
| `--env` | `rustfs.env` | File that sets `S3_SETUP_SQL`, the SQL creating the S3 secret `s3` |
| `--skip-verify` | off | Skip checking results against plain DuckDB |
| `--worker-hosts` | none | Use running workers at these `host:port` addresses instead of starting them |
| `--driver-ssh` | none | Start the driver on this SSH destination; it needs `REMOTE_DUCKDB` |
| `--driver-endpoint` | `localhost:8815` | Address the client attaches to |

| Environment variable | Meaning |
| --- | --- |
| `WORKER_CPUS`, `DRIVER_CPUS` | CPU lists, one per worker; each process gets a systemd scope with that `AllowedCPUs` and a matching `CPUQuota`, which sets DuckDB's thread count |
| `WORKER_MEMORY`, `DRIVER_MEMORY` | `MemoryMax` of those scopes (default `4G`); DuckDB uses 80% of it |
| `DUCKHERDER_STARTUP_SQL` | SQL the driver and workers run in their databases before attaching the data, e.g. `s3_latency.sql` |

The client stays in the cgroup `bench.sh` runs in. Simulated latency applies only to reads that miss DuckDB's buffer pool, so without `--cold` it mostly shows in the first run of each query.

## Output

Each run writes `results/<time>-sf<sf>/`:

- `summary.csv`: median seconds per query, one column per worker count
- `metadata.json`: commit, settings, CPU and memory limits, and startup SQL
- `n<N>.csv`: every timing; `n<N>.log` ends with the SQL each query sent to the driver
- `cold-n<N>/`: per-run logs with `--cold`; `verify-n<N>/`: query and reference results
- `n<N>-driver.log`, `n<N>-worker<i>.log`: process logs

## Results

SF10 with the run command above (`--cold`, one run per query) on an i7-12700K (8 performance cores with two threads each, 4 efficiency cores, 31 GiB). Each DuckDB process gets 2 threads of one performance core and 4 GiB; RustFS gets the 4 efficiency cores. Seconds per query:

| Query | 0 workers | 3 workers | 3 / 0 |
| ---: | ---: | ---: | ---: |
| 1 | 29.67 | 35.08 | 1.18 |
| 2 | 4.96 | 7.61 | 1.53 |
| 3 | 45.66 | 86.67 | 1.90 |
| 4 | 29.45 | 45.58 | 1.55 |
| 5 | 51.99 | 80.34 | 1.55 |
| 6 | 30.95 | 40.75 | 1.32 |
| 7 | 55.08 | 95.74 | 1.74 |
| 8 | 69.85 | 114.08 | 1.63 |
| 9 | 87.72 | 123.01 | 1.40 |
| 10 | 48.71 | 80.50 | 1.65 |
| 11 | 4.45 | 7.48 | 1.68 |
| 12 | 32.47 | 72.46 | 2.23 |
| 13 | 24.94 | 35.18 | 1.41 |
| 14 | 43.69 | 44.97 | 1.03 |
| 15 | 41.22 | 45.56 | 1.11 |
| 16 | 3.35 | 5.66 | 1.69 |
| 17 | 45.57 | 73.79 | 1.62 |
| 18 | 32.12 | 35.95 | 1.12 |
| 19 | 50.43 | 92.06 | 1.83 |
| 20 | 48.84 | 77.22 | 1.58 |
| 21 | 49.86 | 79.02 | 1.58 |
| 22 | 5.90 | 8.58 | 1.46 |
| **Total** | **836.9** | **1287.3** | **1.54** |
