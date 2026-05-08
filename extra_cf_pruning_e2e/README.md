# EXTRA_CF Pruning E2E Workload

This runner generates a TiDB SQL workload for validating CSE EXTRA_CF pruning in
`both` mode. It is intentionally feature-specific:

- stale optimistic transactions are kept open as probes;
- a separate pessimistic `SELECT ... FOR UPDATE; COMMIT` creates lock-only
  EXTRA_CF records for each probe key;
- probe keys are frozen after seeding, so a newer EXTRA_CF record does not hide
  the target record;
- filler workers generate lock-only EXTRA_CF pressure on other keys to push
  records through memtable, L0, and stable sources;
- TiKV metrics are used as the coverage gate.

## Build

```bash
cd /data/code/scripts/extra_cf_pruning_e2e
go test ./...
go build .
```

## Run

The TiKV binary must support `storage.extra-cf-scan-mode=both` (test/testexport
build). The runner attempts to set `both` mode through each TiKV status endpoint
unless `--set-mode=` is passed.

```bash
cd /data/code/scripts/extra_cf_pruning_e2e
go run . \
  --dsn 'root@tcp(127.0.0.1:4000)/test?parseTime=true&interpolateParams=true' \
  --metrics-url http://127.0.0.1:20180/metrics \
  --duration 30m \
  --targets 2000 \
  --pending-probes 128 \
  --filler-workers 32 \
  --max-age-ops 65536 \
  --tikv-log '/data/tidb-data/*/tikv-*/tikv.log'
```

The run is considered successful only if:

- all completed stale probes hit write conflict;
- no `EXTRA_CF both-mode mismatch` appears in the specified TiKV logs;
- `tikv_storage_extra_cf_pruning_datasets_total{phase="selected"}` increases
  for `source="memtable"`, `source="l0"`, and `source="stable"`;
- `tikv_storage_extra_cf_conflict_check_total{mode="both",outcome="write_conflict"}`
  increases.

Pure TiDB SQL cannot force CSE flush or compaction. If the stable or L0 metric
does not grow, the workload did not qualify the physical layout even if all SQL
operations behaved correctly. Increase `--duration`, `--targets`,
`--pending-probes`, `--filler-workers`, or `--max-age-ops`.

## GCP Checklist

Before treating a GCP run as qualifying evidence:

- deploy a TiKV/CSE binary built with `testexport`, or preconfigure a build that
  accepts `storage.extra-cf-scan-mode=both`;
- run the tool from a host that can reach TiDB MySQL and every TiKV status
  endpoint listed in `--metrics-url`;
- pass all TiKV status endpoints, not only one store, if the test cluster has
  multiple TiKV nodes;
- pass `--tikv-log` paths if logs are mounted locally; otherwise rely on TiKV
  crash/SQL failure for mismatch detection and keep the external logs for
  post-run inspection;
- create the database named in the DSN before running, for example `test`.

## Suggested GCP Test Sequence

First run a short smoke test. This checks connectivity, SQL semantics, mode
switching, and metrics scraping without requiring physical source coverage:

```bash
cd /data/code/scripts/extra_cf_pruning_e2e
go run . \
  --dsn 'root@tcp(<tidb-host>:4000)/test?parseTime=true&interpolateParams=true' \
  --metrics-url http://<tikv-0>:20180/metrics \
  --metrics-url http://<tikv-1>:20180/metrics \
  --metrics-url http://<tikv-2>:20180/metrics \
  --duration 5m \
  --targets 500 \
  --pending-probes 64 \
  --filler-workers 16 \
  --max-age-ops 16384 \
  --require-coverage=false
```

Then run the qualification attempt with coverage gates enabled:

```bash
go run . \
  --dsn 'root@tcp(<tidb-host>:4000)/test?parseTime=true&interpolateParams=true' \
  --metrics-url http://<tikv-0>:20180/metrics \
  --metrics-url http://<tikv-1>:20180/metrics \
  --metrics-url http://<tikv-2>:20180/metrics \
  --duration 30m \
  --targets 2000 \
  --pending-probes 128 \
  --filler-workers 32 \
  --max-age-ops 65536
```

Interpretation:

- `PASS`: the run met SQL conflict checks and metrics coverage gates.
- `missing selected l0/stable coverage`: SQL behavior was OK, but the run did
  not qualify the physical layout; increase duration or workload pressure.
- `both-mode write-conflict metric did not increase`: the workload did not hit
  the intended EXTRA_CF both-mode path, or metrics were scraped from the wrong
  TiKV endpoints.
- `stale probes failed with non-conflict errors`: inspect the printed
  `probe_error` lines and TiDB/TiKV logs before treating the result as a pruning
  signal.
- `false negatives observed`: this is a correctness failure until proven
  otherwise.

## Useful Flags

- `--reset=true`: drops and recreates the runner table.
- `--metrics-url`: repeat or comma-separate for multiple TiKV status endpoints.
- `--tikv-log`: repeat, comma-separate, or pass glob patterns.
- `--require-coverage=false`: run SQL workload without failing on metrics gates.
- `--set-mode=`: skip changing TiKV scan mode.
- `--status-interval`: controls progress and metric delta reporting.
