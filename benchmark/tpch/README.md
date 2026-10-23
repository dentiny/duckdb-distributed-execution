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
docker update --cpuset-cpus 10-15 --memory 6g --memory-swap 6g duckherder-rustfs

# Data, once per scale factor. The local file is the reference for result checks.
source benchmark/tpch/rustfs.env && mkdir -p benchmark/tpch/work
TPCH_FILE=benchmark/tpch/work/tpch_sf30.duckdb benchmark/tpch/load.sh 30 s3://duckherder/tpch-sf30

# Benchmark with S3-like latency: client on CPUs 0-1, driver on 8-9, workers on 2-3, 4-5, and 6-7, each with 4 GiB.
DUCKHERDER_STARTUP_SQL="$(cat benchmark/tpch/s3_latency.sql)" \
WORKER_CPUS="2-3 4-5 6-7" WORKER_MEMORY=4G DRIVER_CPUS=8-9 DRIVER_MEMORY=4G \
systemd-run --user --scope -p AllowedCPUs=0-1 -p CPUQuota=200% -p MemoryMax=4G \
  benchmark/tpch/bench.sh --sf 30 --workers "0 1 3" --data-path s3://duckherder/tpch-sf30 --no-load --cold --reps 1
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
| RustFS | 10-15 | 6 GiB |

So each DuckDB process gets both threads of one performance core. CPUs 16-19 stay idle.

- **DuckDB processes.** `bench.sh` starts the driver and each worker in its own systemd scope (`systemd-run --user --scope`), which is a cgroup. `AllowedCPUs` pins the scope to its CPUs. `CPUQuota` is set to 100% per CPU; DuckDB reads it to pick its thread count, because it ignores the cpuset. `MemoryMax` caps the scope's memory, and DuckDB sets its memory limit to 80% of it. The client gets the same limits from the outer `systemd-run` in the run command.
- **RustFS.** RustFS is the S3-compatible object store holding the data. `scripts/local-rustfs.sh` runs it in a Docker container on the same machine, with its data in a Docker volume on the local disk, and serves S3 at `127.0.0.1:19000`. `docker update --cpuset-cpus 10-15 --memory 6g` pins it to the remaining three performance cores, so it serves the processes' reads without becoming the bottleneck. Because storage is local, every request is fast; `s3_latency.sql` adds the delay a real S3 bucket would have (see above).

## Results

SF30 with the run command above (`--cold`, one run per query) on the machine described below. Seconds per query; Speedup is 0 workers over 3 workers. All 22 queries run on the workers; queries 13, 17, 20 and 22 shuffle an input between them. With 1 worker, the worker has the same two CPUs as the driver, so it takes as long as 0 workers.

With simulated S3 latency (`s3_latency.sql`):

![TPC-H SF30 cold start with simulated S3 latency](tpch-sf30-cold.png)

| Query | 0 workers | 1 worker | 3 workers | Speedup |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 18.19 | 18.66 | 6.99 | 2.60x |
| 2 | 3.02 | 3.13 | 2.04 | 1.48x |
| 3 | 27.54 | 27.94 | 11.01 | 2.50x |
| 4 | 17.82 | 18.04 | 8.24 | 2.16x |
| 5 | 30.19 | 30.85 | 11.83 | 2.55x |
| 6 | 17.81 | 18.29 | 6.98 | 2.55x |
| 7 | 32.76 | 33.19 | 12.68 | 2.58x |
| 8 | 34.79 | 35.40 | 13.91 | 2.50x |
| 9 | 49.30 | 50.19 | 16.05 | 3.07x |
| 10 | 28.66 | 29.39 | 14.66 | 1.96x |
| 11 | 3.11 | 3.03 | 1.85 | 1.68x |
| 12 | 19.89 | 20.25 | 8.40 | 2.37x |
| 13 | 14.07 | 14.15 | 7.06 | 1.99x |
| 14 | 25.52 | 25.14 | 9.91 | 2.58x |
| 15 | 26.58 | 27.26 | 10.03 | 2.65x |
| 16 | 2.35 | 2.42 | 2.18 | 1.08x |
| 17 | 22.85 | 22.81 | 11.86 | 1.93x |
| 18 | 25.32 | 25.34 | 25.33 | 1.00x |
| 19 | 27.86 | 27.66 | 10.86 | 2.57x |
| 20 | 27.75 | 28.14 | 11.86 | 2.34x |
| 21 | 25.40 | 25.74 | 10.33 | 2.46x |
| 22 | 4.14 | 5.30 | 3.06 | 1.35x |
| **Total** | **484.9** | **492.3** | **217.1** | **2.23x** |

Without simulated latency:

![TPC-H SF30 cold start without latency](tpch-sf30-cold-no-latency.png)

| Query | 0 workers | 1 worker | 3 workers | Speedup |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 4.64 | 4.32 | 3.12 | 1.49x |
| 2 | 0.61 | 0.61 | 0.69 | 0.88x |
| 3 | 5.47 | 5.54 | 4.48 | 1.22x |
| 4 | 3.79 | 3.85 | 3.68 | 1.03x |
| 5 | 5.60 | 5.64 | 4.68 | 1.19x |
| 6 | 3.17 | 3.34 | 2.94 | 1.08x |
| 7 | 5.41 | 5.94 | 5.07 | 1.07x |
| 8 | 5.81 | 5.87 | 5.17 | 1.12x |
| 9 | 9.82 | 9.96 | 6.87 | 1.43x |
| 10 | 5.71 | 5.74 | 6.21 | 0.92x |
| 11 | 0.59 | 0.60 | 0.69 | 0.86x |
| 12 | 3.92 | 3.95 | 3.34 | 1.17x |
| 13 | 6.30 | 6.33 | 3.53 | 1.79x |
| 14 | 4.40 | 4.37 | 4.20 | 1.05x |
| 15 | 4.28 | 4.49 | 4.07 | 1.05x |
| 16 | 1.02 | 1.03 | 1.22 | 0.84x |
| 17 | 4.26 | 4.32 | 7.21 | 0.59x |
| 18 | 7.25 | 7.41 | 7.41 | 0.98x |
| 19 | 5.01 | 5.06 | 4.39 | 1.14x |
| 20 | 5.06 | 5.21 | 5.34 | 0.95x |
| 21 | 5.71 | 5.73 | 4.38 | 1.30x |
| 22 | 1.28 | 1.46 | 1.40 | 0.92x |
| **Total** | **99.1** | **100.8** | **90.1** | **1.10x** |
