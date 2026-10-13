# Join partition cost benchmark

Compare a CLI built before the join cost change with a CLI built after it. Both must include the same RPC and catalog fixes. The worker binary can be shared because the change only affects driver planning.

```sh
# Save the current-main CLI before applying/building the cost change.
cmake --build build/reldebug --target shell distributed_worker --parallel 4
cp build/reldebug/duckdb /tmp/duckdb-before-join-cost

# Apply the cost change, then rebuild the CLI.
cmake --build build/reldebug --target shell --parallel 4
python3 benchmark/join_cost/run.py \
  --before /tmp/duckdb-before-join-cost \
  --after build/reldebug/duckdb \
  --worker-binary build/reldebug/extension/duckherder/distributed_worker \
  --out /tmp/join-cost-results \
  --data-root /tmp/join-cost-objfs
```

The script creates one local ObjFS database, computes reference results with native DuckDB, then starts separate worker processes and a fresh driver for each comparison round. It checks every query result against the reference and records actual task counts from execution statistics. Only processes started by the script are stopped.

Default configuration: four driver threads, four workers with one thread each, two warmups and five measured runs per case, repeated in two rounds with the before/after order reversed. Medians therefore contain ten samples per implementation and case. Timings use DuckDB JSON profiling latency, including planning, RPCs and result consumption. The profiler is enabled in both implementations; CLI wall timings are also retained. These are warm-cache local measurements with no CPU pinning. They do not measure remote-storage latency or cluster scaling.

Cases:

- **selective:** a four-million-row fact is clustered by category; an equality filter leaves roughly 4,000 rows. The other input has one million rows. The old row-group policy partitions the physically larger fact, leaving almost all filtered work in one task. The cost policy can replicate the filtered fact and partition the other input.
- **wide:** a one-million-row narrow input joins a 750,000-row input with 64 projected BIGINT payload columns. The width estimate can reverse the partition side despite the smaller row-group count.
- **large_replica:** two two-million-row inputs use permuted join keys, so a rowid slice cannot narrow the replicated side's storage range. The replicated hash-build estimate exceeds the default cost guard and execution stays on the driver.

`--rows` changes the base data scale; `--workers`, `--driver-threads`, `--worker-threads`, `--warmup`, `--reps` and `--rounds` change the execution setup. Changing these may change the cost policy's decisions, so inspect `execution.json` as well as timings.

Outputs include `summary.csv`, all samples in `timings.csv`, reference results and task counts in `execution.json`, binary SHA-256 values and configuration in `metadata.json`, per-query JSON profiles, and complete driver/worker logs.

## Initial cost policy

For each direct join input, use its filtered estimated cardinality and projected tuple width. Width includes tuple validity and small variable-size penalties; a hash-build estimate adds the stored hash and average hash-table entries. These estimates are not exact memory bounds.

Partition the input with larger estimated scan bytes. Let `P` be its scan bytes, `R` the replicated input's scan bytes, `B` the replicated hash-build bytes and `N` the actual task count, capped by row groups. Only partition when all of these hold:

- `N >= 2`;
- `B <= 64 MiB`;
- `(N - 1) * (R + B) < P * (1 - 1 / N)`.

The inequality compares duplicate scan/build work with estimated parallel scan savings. Missing cardinality estimates retain driver execution. The same decision runs when the optimizer creates the fragment and when execution refreshes its plan. Storage ranges still use actual row-group boundaries.
