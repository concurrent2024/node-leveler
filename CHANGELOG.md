# Changelog

## v0.1.9

- The default `SMTP_PORT` is 465 (implicit TLS, the common case) instead of 994. Nothing
  changes for deployments that set the key — an explicit value always wins — but the
  default no longer points at one provider's port, which read as a fingerprint in a public
  config.
- `LICENSE` copyright holder is now the project account rather than a personal name.
- The release tooling (desensitize scanner + publish script) leaves the repository: it is
  our process, not the product, and it lives outside the tree so one copy serves every
  project and the private vocabulary file never sits next to the code. CI keeps a small
  built-in hygiene check (private IPs, local paths, non-`example.com` mail domains).

## Unreleased

- Tables are aligned by display width, not by character count. A Chinese header is four
  characters but eight columns, so `len()`-based padding left every CJK table ragged; widths
  are now measured with `unicodedata.east_asian_width`, columns are separated by a fixed
  gap, and columns whose cells are all numeric are right-aligned so digits stack. Three
  tests cover wide-character widths, CJK/latin alignment and numeric right-alignment.

## Unreleased

- Alert bodies can be Chinese: `NOTIFY_LANG=zh` (or `en`, the default). Every label,
  table header, next-step line and the self-check footer comes from one translation table,
  so a deployment picks a language without touching code; an unknown value falls back to
  English. The subject keeps the stable English reason id (`pool-slot`, `node-hot`, ...)
  for filtering, with severity and headline number unchanged.
- Three tests cover the Chinese template, the unknown-language fallback, and that both
  language tables define exactly the same keys.

## Unreleased

- Alert bodies are plain text that a mail client can actually read: aligned columns, one
  blank line between blocks, a header line with cluster, severity, timestamp, mode and job
  name, and a two-command self-check footer added in one place. Markdown (`**`, backticks,
  `###`) no longer leaks into the message, where it used to show up literally.
- Subjects carry severity and the headline number, so the inbox can be triaged without
  opening the mail: `[node-leveler][prod-cluster][WARN] pool-slot 98.6% (general/4C 136/138)`.
  Levels: abort/auto-downgrade CRIT, capacity/pool-slot/node-hot WARN.
- Pool alerts include a trend line built from the last six rounds' pool waterlines, which
  are now kept in the state ConfigMap (`pool_hist`).
- Tests: subject level mapping, header fields, table alignment and plainness, no Markdown in
  the body, footer presence.

## Unreleased

- Alerts can now go out by email as well as DingTalk, and the two channels are independent:
  each has its own enable flag, its own 180-minute throttle key, and a failure in one never
  suppresses the other. Plain-text SMTP via the standard library (`smtplib`), implicit TLS
  (`SMTP_SECURITY=ssl`, port 994 for Coremail) or STARTTLS; credentials only from a Secret
  (`SMTP_USER` / `SMTP_PASS`), the rest of the settings live in the ConfigMap. A mail problem
  is logged as `notify-failed` and never stops a balancing round.
- Six regression tests cover subject/body construction, STARTTLS selection, missing
  credentials, a refused connection, per-channel throttling and `force=True`.

## Unreleased

- The state ConfigMap is now created-and-then-updated correctly. On a brand new cluster the
  first write was a POST; every later write reused the stale "no ConfigMap" handle, POSTed
  again and got 409 AlreadyExists. The round still reported success, so the damage was
  silent: the single-instance lock was never released (later rounds exited as `locked`) and
  no move, cooldown or EMA sample was ever persisted. `save_state()` now re-checks the
  object and sends PUT with the current resourceVersion; `state_written` in the summary line
  is the HTTP code, so 200 means the write really landed.
- Three regression tests cover create-then-update, resourceVersion on update, and that an
  existing object is never POSTed again.

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
- Cluster-free test suite (159 tests) covering the decision logic and the regression
  cases: cordon add/heal (release only our own locks), settle detection (never mistake a
  sibling replica for the replacement), "was this workload already broken", EMA
  smoothing, target selection, taint/affinity matching and the landing guard
- Configuration reference generated from the chart values
- MIT license
