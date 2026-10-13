#!/usr/bin/env python3
"""Compare two Duckherder CLIs on selective, wide and expensive replicated joins."""
import argparse
import csv
import hashlib
import json
import os
import platform
import re
import socket
import statistics
import subprocess
import time
from pathlib import Path


def quoted(value):
    return "'" + str(value).replace("'", "''") + "'"


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def run_cli(binary, sql, log, threads):
    env = os.environ.copy()
    env["DUCKHERDER_STARTUP_SQL"] = f"SET GLOBAL threads = {threads};"
    result = subprocess.run([str(binary), "-init", "/dev/null", "-batch", "-bail"],
                            input=sql, text=True, capture_output=True, env=env, timeout=600)
    log.write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(f"CLI failed: see {log}")
    return result.stdout


def setup_sql(root, rows):
    wide_columns = ", ".join(f"i * {col + 1} AS v{col}" for col in range(64))
    return f"""LOAD duckdb_object_storage;
SET threads = 4;
SET GLOBAL duckdb_objfs_backend = 'local';
SET GLOBAL duckdb_objfs_root = {quoted(root)};
ATTACH 'duckdb_objfs://join_cost.db' AS object_db;
USE object_db;
CREATE OR REPLACE TABLE selective_fact AS
  SELECT i % 8192 AS k, i // {max(1, rows // 250)} AS category, i % 11 AS amount
  FROM range({rows * 4}) t(i);
CREATE OR REPLACE TABLE selective_dim AS
  SELECT i % 8192 AS k, i % 7 AS amount FROM range({rows}) t(i);
CREATE OR REPLACE TABLE wide_fact AS
  SELECT i % 65536 AS k, i % 11 AS amount FROM range({rows}) t(i);
CREATE OR REPLACE TABLE wide_dim AS
  SELECT i % 65536 AS k, {wide_columns} FROM range({rows * 3 // 4}) t(i);
CREATE OR REPLACE TABLE replica_fact AS
  SELECT (i * 104729) % {rows * 2} AS k, i % 11 AS amount FROM range({rows * 2}) t(i);
CREATE OR REPLACE TABLE replica_dim AS
  SELECT (i * 104729) % {rows * 2} AS k, i % 7 AS amount FROM range({rows * 2}) t(i);
ANALYZE;
CHECKPOINT;
"""


def queries():
    wide_sum = " + ".join(f"d.v{col}" for col in range(64))
    return {
        "selective": "SELECT sum(f.amount + d.amount) FROM selective_fact f "
                     "JOIN selective_dim d ON f.k = d.k WHERE f.category = 42;",
        "wide": f"SELECT sum(f.amount + {wide_sum}) FROM wide_fact f "
                "JOIN wide_dim d ON f.k = d.k;",
        "large_replica": "SELECT sum(f.amount + d.amount) FROM replica_fact f "
                         "JOIN replica_dim d ON f.k = d.k;",
    }


def parse_runs(output, warmup, expected):
    current = None
    timings = []
    values = {}
    for line in output.splitlines():
        if line.startswith("case="):
            name, run = line.split()
            current = (name.split("=", 1)[1], int(run.split("=", 1)[1]))
        elif current and re.fullmatch(r"-?\d+", line.strip()):
            name, run = current
            value = int(line)
            if value != expected[name]:
                raise RuntimeError(f"Incorrect {name} result: {value} != {expected[name]}")
            values[current] = value
        elif current and line.startswith("Run Time"):
            match = re.search(r"real\s+([0-9.]+)", line)
            if not match or current not in values:
                raise RuntimeError("Missing timing or result")
            name, run = current
            if run >= warmup:
                timings.append((name, run - warmup, float(match.group(1))))
            current = None
    return timings


def run_cluster(args, binary, label, round_id, expected):
    processes, files = [], []
    ports = []
    try:
        for index in range(args.workers):
            port = free_port()
            ports.append(port)
            log = (args.out / f"{label}-r{round_id}-worker{index}.log").open("w")
            files.append(log)
            env = os.environ.copy()
            env["DUCKHERDER_STARTUP_SQL"] = f"SET GLOBAL threads = {args.worker_threads};"
            process = subprocess.Popen([str(args.worker_binary), "127.0.0.1", str(port), f"w{index}"],
                                       env=env, stdout=log, stderr=subprocess.STDOUT)
            processes.append(process)
            deadline = time.monotonic() + 30
            while True:
                if process.poll() is not None:
                    raise RuntimeError("Worker exited; see worker logs")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() > deadline:
                        raise RuntimeError("Worker startup timed out")
                    time.sleep(0.05)
        driver_port = free_port()
        commands = ["LOAD duckherder;", ".headers off", ".mode csv",
                    f"SELECT duckherder_start_local_server({driver_port}, 0);"]
        commands += [f"SELECT duckherder_register_worker('w{i}', 'grpc://127.0.0.1:{port}');"
                     for i, port in enumerate(ports)]
        commands += [f"ATTACH '127.0.0.1:{driver_port}/join_cost.db' AS dh "
                     f"(TYPE duckherder, READ_ONLY, DATA_PATH {quoted(args.data_root)});", "USE dh;", "PRAGMA enable_profiling='json';", ".timer on"]
        for name, sql in queries().items():
            for run in range(args.warmup + args.reps):
                profile = args.out / "profiles" / f"{label}-r{round_id}-{name}-{run}.json"
                commands += [f"SET profiling_output={quoted(profile)};", f".print case={name} run={run}", sql]
        stats_file = args.out / f"{label}-r{round_id}-stats.csv"
        commands += [".timer off", "PRAGMA disable_profiling;", f"COPY (SELECT sql, execution_mode, num_tasks_generated "
                     f"FROM duckherder_get_query_execution_stats()) TO {quoted(stats_file)} (HEADER, FORMAT CSV);",
                     "USE memory;", "DETACH dh;", "SELECT duckherder_stop_local_server();"]
        output = run_cli(binary, "\n".join(commands), args.out / f"{label}-r{round_id}.log", args.driver_threads)
        wall_runs = parse_runs(output, args.warmup, expected)
        runs = []
        for name, run, wall_time in wall_runs:
            profile = json.loads((args.out / "profiles" / f"{label}-r{round_id}-{name}-{run + args.warmup}.json").read_text())
            if profile["rows_returned"] != 1 or profile["latency"] <= 0:
                raise RuntimeError("Invalid query profile")
            runs.append((name, run, profile["latency"], wall_time))
        if len(runs) != len(queries()) * args.reps:
            raise RuntimeError("Unexpected number of measured queries")
        with stats_file.open() as source:
            stats = list(csv.DictReader(source))
        modes = {}
        for name, table in (("selective", "selective_fact"), ("wide", "wide_fact"),
                            ("large_replica", "replica_fact")):
            tasks = [int(row["num_tasks_generated"]) for row in stats if table in row["sql"]]
            if not tasks:
                raise RuntimeError(f"Missing execution statistics for {name}")
            modes[name] = max(tasks)
        return runs, modes
    finally:
        # These are the read-only worker processes started by this invocation.
        for process in processes:
            if process.poll() is None:
                process.kill()
            process.wait()
        for log in files:
            log.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--before", type=Path, required=True)
    parser.add_argument("--after", type=Path, required=True)
    parser.add_argument("--worker-binary", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000000)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--driver-threads", type=int, default=4)
    parser.add_argument("--worker-threads", type=int, default=1)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--reps", type=int, default=5)
    parser.add_argument("--rounds", type=int, default=2)
    args = parser.parse_args()
    if args.rows < 400000 or args.workers < 2 or min(args.reps, args.rounds, args.driver_threads, args.worker_threads) < 1 or args.warmup < 0:
        parser.error("rows >= 400000, workers >= 2, positive repetitions/threads, warmup >= 0 required")
    for name in ("before", "after", "worker_binary", "out", "data_root"):
        setattr(args, name, getattr(args, name).resolve())
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "profiles").mkdir(exist_ok=True)
    args.data_root.mkdir(parents=True, exist_ok=True)
    (args.out / "metadata.json").write_text(json.dumps({
        **{key: str(value) if isinstance(value, Path) else value for key, value in vars(args).items()},
        "platform": platform.platform(), "cpu_count": os.cpu_count(),
        "binary_sha256": {name: hashlib.sha256(getattr(args, name).read_bytes()).hexdigest()
                          for name in ("before", "after", "worker_binary")},
        "timing": "DuckDB profiling latency with CLI wall time also recorded; warm caches, separate workers, no CPU pinning",
    }, indent=2) + "\n")
    print("Creating shared ObjFS data", flush=True)
    sql = setup_sql(args.data_root, args.rows)
    (args.out / "setup.sql").write_text(sql)
    run_cli(args.after, sql, args.out / "setup.log", args.driver_threads)
    reference_sql = f"""LOAD duckdb_object_storage;
SET threads = {args.driver_threads};
SET GLOBAL duckdb_objfs_backend = 'local';
SET GLOBAL duckdb_objfs_root = {quoted(args.data_root)};
ATTACH 'duckdb_objfs://join_cost.db' AS object_db (READ_ONLY);
USE object_db;
.headers off
.mode csv
""" + "\n".join(queries().values())
    reference = run_cli(args.after, reference_sql, args.out / "reference.log", args.driver_threads)
    values = [int(line) for line in reference.splitlines() if re.fullmatch(r"-?\d+", line.strip())]
    if len(values) != len(queries()):
        raise RuntimeError("Missing native reference results")
    expected = dict(zip(queries(), values))
    all_runs, all_modes = [], {}
    for round_id in range(args.rounds):
        labels = ["before", "after"] if round_id % 2 == 0 else ["after", "before"]
        for label in labels:
            print(f"Round {round_id}: {label}", flush=True)
            runs, modes = run_cluster(args, getattr(args, label), label, round_id, expected)
            print(f"  tasks: {modes}", flush=True)
            all_modes[f"{label}-r{round_id}"] = modes
            all_runs += [(label, round_id, *run) for run in runs]
    with (args.out / "timings.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("policy", "round", "case", "run", "seconds", "cli_seconds"))
        writer.writerows(all_runs)
    (args.out / "execution.json").write_text(json.dumps({"reference_results": expected, "tasks": all_modes}, indent=2) + "\n")
    with (args.out / "summary.csv").open("w", newline="") as output:
        writer = csv.writer(output)
        writer.writerow(("case", "before_median_s", "after_median_s", "speedup"))
        for name in queries():
            medians = [statistics.median(row[4] for row in all_runs if row[0] == label and row[2] == name)
                       for label in ("before", "after")]
            speedup = medians[0] / medians[1]
            writer.writerow((name, *medians, speedup))
            print(f"{name}: {medians[0]:.6f}s -> {medians[1]:.6f}s ({speedup:.2f}x)")
    print(f"Results: {args.out}")


if __name__ == "__main__":
    main()
