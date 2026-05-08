# EXTRA_CF Pruning E2E Runner Tracking

## Spec

The runner should provide a TiDB-level E2E workload that validates the CSE
EXTRA_CF pruning write-conflict path under `both` mode.

Done when:

- the workload creates stale optimistic transactions whose target conflict is a
  lock-only EXTRA_CF record;
- target keys are not modified again before probing;
- filler traffic can move the target record through CSE physical sources;
- the runner fails on SQL false negatives;
- the runner fails on `both` mode mismatch logs;
- the runner fails if selected memtable, L0, and stable dataset metrics do not
  all increase.

## Current Decisions

- Implemented as a standalone Go module under `/data/code/scripts` to avoid
  coupling it to the existing Rust helper workspace.
- Uses only TiDB SQL for data operations.
- Uses TiKV/CSE metrics as the physical-layout oracle because TiDB SQL cannot
  directly control or observe memtable flush and stable compaction.
- Defaults to setting `storage.extra_cf_scan_mode=both` via TiKV status API.

## Validation

- `go test ./...`
- `go vet ./...`
- `go build -o /tmp/extra-cf-pruning-e2e .`
- `/tmp/extra-cf-pruning-e2e --help`

## Readiness

Ready for the first GCP smoke run, but not yet qualified by a real cluster run.
The first GCP run should use short duration with coverage gates enabled to prove
the runner can reach TiDB, set `both` mode, read all TiKV metrics endpoints, and
observe selected memtable/L0/stable datasets.
