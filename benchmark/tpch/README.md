# TPC-H benchmark over ObjFS

Runs the 22 TPC-H queries through a duckherder driver with 0..N workers, checks the results, and times them. Data lives in an ObjFS database on S3-compatible storage (local RustFS by default).

Timings are only meaningful on Linux, where each process runs in its own cgroup.

## Build

Requires `pkg-config`, `flex`, `bison`, `libtool`, `cargo`, vcpkg, and Docker and curl 7.75 or later for RustFS. Set `LATENCY_INJECTION_FS_DIR` to a [duckdb-filesystem-latency-injection](https://github.com/dentiny/duckdb-filesystem-latency-injection) checkout to build it in for simulating storage latency; leave it unset otherwise.

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
| `--queries` | all 22 | Queries to run, e.g. `"1 6 12"` or `1,6,12`; also limits the result check |
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

The client stays in the cgroup `bench.sh` runs in.

## Simulated latency

`s3_latency.sql` wraps `SlateDBFileSystem`, the filesystem `duckdb_object_storage` registers for the ObjFS database, with [latency_inject_fs](https://github.com/dentiny/duckdb-filesystem-latency-injection). Each call sleeps before running the real I/O against local RustFS. The driver and every worker wrap their own filesystem.

| Operation | Delay (log-normal) | Mean | Standard deviation |
| --- | --- | ---: | ---: |
| Read | base, plus `bytes / 88000` ms | 30 ms | 15 ms |
| Stat | base | 20 ms | 10 ms |
| List | base | 40 ms | 20 ms |

The values approximate S3 Standard within a region.

## Output

The console prints each query number as it runs. Each run writes `results/<time>-sf<sf>/`:

- `summary.csv`: median seconds per query, one column per worker count
- `metadata.json`: commit, settings, CPU and memory limits, and startup SQL
- `n<N>.csv`: every timing
- `runs-n<N>/`: `q<query>.log` and `.csv` per query, or `q<query>-r<run>` per run with `--cold`; each log ends with the SQL the query sent to the driver
- `verify-n<N>/`: query and reference results
- `n<N>-driver.log`, `n<N>-worker<i>.log`: process logs

## Test setup

Everything runs on one machine, an i7-12700K with 31 GiB of memory. Its CPUs 0-15 are the 8 performance cores with two threads each, and CPUs 16-19 are the 4 efficiency cores. Each process is pinned to its own CPUs so the processes don't compete with each other:

| Process | CPUs | Memory limit |
| --- | --- | --- |
| Client (`bench.sh` and the DuckDB shell it runs) | 0-1 | 4 GiB |
| Worker 1, 2, 3 | 2-3, 4-5, 6-7 | 4 GiB each |
| Driver | 8-9 | 4 GiB |
| RustFS | 16-19 | 2 GiB |

So each DuckDB process gets both threads of one performance core. CPUs 10-15 stay idle.

- **DuckDB processes.** `bench.sh` starts the driver and each worker in its own systemd scope (`systemd-run --user --scope`), which is a cgroup. `AllowedCPUs` pins the scope to its CPUs. `CPUQuota` is set to 100% per CPU; DuckDB reads it to pick its thread count, because it ignores the cpuset. `MemoryMax` caps the scope's memory, and DuckDB sets its memory limit to 80% of it. The client gets the same limits from the outer `systemd-run` in the run command.
- **RustFS.** RustFS is the S3-compatible object store holding the data. `scripts/local-rustfs.sh` runs it in a Docker container on the same machine, with its data in a Docker volume on the local disk, and serves S3 at `127.0.0.1:19000`. `docker update --cpuset-cpus 16-19 --memory 2g` pins it to the efficiency cores. Because storage is local, every request is fast; `s3_latency.sql` adds the delay a real S3 bucket would have (see above).

## Results

SF10 with the run command above (`--cold`, one run per query) on the machine described below. Seconds per query with simulated S3 latency (`s3_latency.sql`). Queries 2, 13, 20 and 22 run on the driver; the others run on the workers.

![TPC-H SF10 cold start](tpch-sf10-cold.png)

| Query | 0 workers | 3 workers | Speedup |
| ---: | ---: | ---: | ---: |
| 1 | 28.97 | 10.60 | 2.73x |
| 2 | 4.51 | 4.44 | 1.02x |
| 3 | 45.08 | 16.69 | 2.70x |
| 4 | 28.06 | 10.46 | 2.68x |
| 5 | 49.38 | 19.01 | 2.60x |
| 6 | 29.80 | 10.63 | 2.80x |
| 7 | 54.32 | 19.64 | 2.77x |
| 8 | 61.66 | 22.41 | 2.75x |
| 9 | 67.73 | 26.68 | 2.54x |
| 10 | 47.20 | 22.23 | 2.12x |
| 11 | 4.10 | 3.60 | 1.14x |
| 12 | 30.64 | 10.95 | 2.80x |
| 13 | 22.32 | 22.83 | 0.98x |
| 14 | 44.06 | 15.89 | 2.77x |
| 15 | 40.74 | 14.77 | 2.76x |
| 16 | 2.69 | 2.09 | 1.29x |
| 17 | 42.30 | 32.68 | 1.29x |
| 18 | 30.05 | 15.67 | 1.92x |
| 19 | 49.23 | 17.40 | 2.83x |
| 20 | 46.60 | 46.82 | 1.00x |
| 21 | 44.12 | 16.42 | 2.69x |
| 22 | 5.69 | 5.75 | 0.99x |
| **Total** | **779.3** | **367.7** | **2.12x** |
