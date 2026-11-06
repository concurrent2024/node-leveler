# Changelog

## Unreleased

- SOURCE_RELIEF (a minimum relief in percentage points) is replaced by RELIEF_CORES and
  RELIEF_GI, absolute resource amounts. Percentage points are machine-size dependent: one
  eviction relieves 2.2 points on a 46-slot node but 0.47 on a 211-slot node, 12.5 points of
  CPU on 4 cores but 3.1 on 16, so a point floor is unreachable on large machines. That
  froze a whole 16C pool for hours: the pool was detected as a source, every candidate was
  rejected, and every round ended idle.
- Slot dimensions no longer consult any benefit floor at all (one eviction is always one
  slot); they are throttled by MAX_PER_CYCLE / MAX_PER_NODE / DAILY_BUDGET.
- Regression tests pin the behaviour: a 211-slot source moves with any floor value, a tiny
  CPU relief is rejected as cores, and a 0.2 GiB relief is rejected as GiB.

## v0.1.0

First public release.

- Eviction-only node balancing driven by pod slots, real CPU/memory and requests
- Pool-pressure driver: when a whole pool crosses the slot waterline, its fullest nodes
  become migration sources even if no single node is over its own line
- Cordon-guided placement with `finally`-block release and TTL self-healing; disabled for
  StatefulSets, where PV zone affinity leaves too few candidate nodes
- Memory guards measured against physical capacity rather than allocatable, plus a
  physical hard cap on every landing
- Quotas per round / node / namespace / single-replica / StatefulSet / day, plus a
  per-workload cooldown and a single-instance lock
- Automatic downgrade to dry-run when a moved pod fails to start; workloads that were
  already failing are only cooled down
- Standalone single-file implementation (Python standard library only) with a Helm chart;
  the state ConfigMap stays outside the release so a reinstall keeps its cooldowns
- Cluster-free test suite (132 tests) covering the decision logic and the regression
  cases: cordon add/heal (release only our own locks), settle detection (never mistake a
  sibling replica for the replacement), "was this workload already broken", EMA
  smoothing, target selection, taint/affinity matching and the landing guard
- Configuration reference generated from the chart values
- MIT license
