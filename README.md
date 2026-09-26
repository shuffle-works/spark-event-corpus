# spark-event-corpus

Generates real Spark event logs by running real Spark jobs in Docker, not by
synthesizing JSON, across Spark versions, table formats, and a bounded
scenario matrix. It also sources additional real logs from public
repositories. The result is a corpus for testing
[sparkforensics](https://github.com/shuffle-works/sparkforensics) against
workloads whose bottlenecks are known in advance.

The generator lives here; the logs themselves live in a sibling data repo (see
below). `index.json` is the catalog tying the two together: one entry per log,
recording its id, path, checksum, source, and, for generated logs, the config
that produced it, the detector tags it makes fire (`targets_detectors`), the
tags it was designed for but does not reach, each with the measured reason
(`known_misses`), and the tags that actually fire on it (`fires_detectors`,
see [Check the detector tags](#check-the-detector-tags)).

## Prerequisites

- Docker (the workload runs on a throwaway Spark standalone cluster) and
  network access (Spark images, Maven artifacts, the Apache dist listing).
- **A sibling `spark-event-corpus-data` git repo**, cloned next to this one:

      <parent>/
        spark-event-corpus/        # this repo: generator + catalog
        spark-event-corpus-data/   # the logs themselves

  The generation scripts write logs into `spark-event-corpus-data/logs/`. It
  has to be a real clone, not just a directory, because the corpus is
  committed and tagged there by hand once generation finishes, and every
  catalog entry's `data_repo_tag` refers to that tag.

All scripts under `scripts/` are meant to be run by hand, not in CI.

## Setup

    python3 -m venv .venv && source .venv/bin/activate
    pip install -r requirements.txt

## Run the tests

    pytest

Unit tests only. They cover the matrix, catalog, validation, the detector-tag
check, and the compose/env contract, and need neither Docker, Node, nor network.

## Generate the corpus

    python3 scripts/run_generation.py
    python3 scripts/fetch_external.py

**Both are safe to re-invoke.** Each skips ids already present in
`index.json` before doing any work, so a run interrupted by a container
failure, a flaky download, or a machine reboot can simply be started again and
picks up where it left off. A restart also keeps stamping the tag the earlier
invocation used rather than a fresh date-based one, so entries from a single
generation run stay on one tag even if it spans days.

`--tag` sets the data repo tag new entries are stamped with. Without it, new
entries reuse the tag the catalog's existing entries carry, which is right for
a restart but wrong when adding runs to a corpus whose logs are already
tagged: pass a fresh tag then, and pass the same one again on any restart.

To regenerate some runs, delete their entries from `index.json` first; every
other run is skipped as already done. `--data-repo` writes the logs into a
different `spark-event-corpus-data` clone than the sibling one.

### Expect 24 generated runs, not 25

`run_generation.py` covers four Spark minor lines (3.5, 4.0, 4.1, 4.2) times
three table formats = 12 baselines, plus 7 pairwise scenario runs, 4 failure
scenario runs and 2 cache scenario runs on the latest version = 25. One of
those, **Spark 4.2 + Iceberg, is deliberately skipped**: Iceberg has not
published a Spark 4.2 runtime artifact yet, so there is nothing to run
against. The script prints
`SKIPPED: ... no upstream table-format artifact available yet` and continues.
A 24-of-25 count is the expected outcome, not a failure. It becomes 25 on its
own once upstream Iceberg ships that artifact and `TABLE_FORMAT_ARTIFACTS` in
`src/corpus/table_formats.py` gains a `4.2` to `iceberg` entry.

## Check the detector tags

    python3 scripts/verify_detectors.py [--data-repo <path>]

Runs `sparkforensics-analyze <log> --format json` over every self-generated
log, writes the tags of the findings it reports into each entry's
`fires_detectors`, and stamps `fires_detectors_checked_with` with the exact CLI
it used. The CLI is pinned (`SPARKFORENSICS_CLI_VERSION` in
`src/corpus/detectors.py`) and fetched from npm through `npx`, so the only
extra prerequisite is Node. `--data-repo` defaults to the sibling
`spark-event-corpus-data` clone. Run it after generating, and again after
bumping the pinned version, then commit the updated `index.json`.

A tag counts as fired only when it appears in `findings`; tags listed under
`cleanChecks` ran and found nothing. External logs have no targets and are
skipped. Before analyzing, each log's checksum is compared with its catalog
entry, so the observed tags always describe the cataloged log. The analyzer
exits `3` ("inconclusive") on every log without an application-end event,
such as `failure-killed-run`, but still prints its full report, so that exit
counts as a successful analysis.

Exit codes: `0` every targeted tag fired, `1` at least one targeted tag did
not fire (each is printed as `MISSED`), `2` at least one log could not be
checked (missing, checksum mismatch, or analyzer failure). `index.json` is
rewritten either way.

Against sparkforensics-cli 0.2.4, 43 of the 45 targeted scenario and tag
pairs fire, so the script currently exits `1`. Every pairwise and failure
scenario target fires; the two misses are the cache runs, which target `CSTOR`
as rebuilt on `SparkListenerBlockUpdated` events, which 0.2.4 predates. A tag
listed under an entry's `known_misses` is not a target, so it never counts as
missed.

## The scenario matrix

Beyond the per-version baselines, seven pairwise runs vary seven binary axes
(AQE, shuffle partitions, dynamic allocation, speculation, slow host, skew,
caching) over a Taguchi L8 orthogonal array: the minimal design in which every
pair of axis settings occurs at least once, so 7 runs cover what 128 would
exhaustively. Each row declares the detector tags it targets; see
`src/corpus/matrix.py`.

Two details are load-bearing and easy to undo by accident, so
`tests/test_compose_contract.py` pins them:

- **Broadcast joins are disabled**
  (`spark.sql.autoBroadcastJoinThreshold=-1`). The workload's dimension tables
  are small enough that Spark would otherwise broadcast every join, producing
  no shuffle at all, which in turn makes the shuffle-partitions axis inert and
  leaves the shuffle, spill, partitioning, and skew detectors with nothing to
  find.
- **Every worker advertises an explicit core count** (`--cores`), and the
  slow-host axis throttles worker-2's *CPU quota* instead: on a slow-host row
  it takes 6 task slots on 0.3 CPU. Left to auto-detect, the Worker JVM reads
  its core count off the container's cgroup quota, so a "slow" host would
  merely be handed fewer tasks while running each one at full speed, giving
  the host and straggler detectors no per-task slowdown to see.

### Scaled to clear the detector floors

The baseline workload is too small for most detector floors, so the pairwise
rows scale it up, each knob for the floor it clears (`PAIRWISE_SCALE` and the
rows in `src/corpus/matrix.py`):

| Knob | Rows | Clears |
|---|---|---|
| a third worker, `spark-worker-3`, 2 CPU and 2 slots | all | `HOST` needs 3 hosts or executors |
| worker-2 advertises 6 slots on 0.3 CPU | slow-host rows 01, 03, 04, 06 | `HOST` on short tasks; 6 speculated tail tasks for `SPEC` |
| `spark.default.parallelism=20` | all | `HOST` needs a stage of 15 or more tasks |
| 8 extra columns of `rand()` doubles | all but 01 | `SHFL`, `PART` |
| 10M rows; 16M on 03 and 07; 50M narrow rows on 01 | per row | `SHFL`, `PART`; on 01 the hot task outlasts the speculated tail |
| one hot key holding 80% of rows | skew rows 01, 02, 05, 06 | `PART`; `SPEC` on 01 |
| 4 join keys instead of 1000 | skew-free 2000-partition rows 03, 07 | `PART` without injected skew |
| workers sleep 45 s before starting | cold-start rows 02, 03, 04, 05 | `COLD` (first executor over 30 s after the first stage), `UTIL` |
| `spark.memory.fraction=0.02` | spill rows 04, 05, 06, 07 | `SPILL` |
| the fact table is written to Parquet, read back, and counted again after the join | caching rows 01, 02, 04, 07 | `CACHE` counts file-scan relations read by two or more SQL executions |
| `spark.eventLog.logBlockUpdates.enabled=true` | caching rows 01, 02, 04, 07 | gives `CSTOR` the cached partitions to measure |
| `spark.speculation.minTaskRuntime=12s` | speculation rows 01, 03, 05, 07 | `SPEC` needs its losers to add up to 60 s |

The baselines, failure and cache scenarios keep the unscaled two-worker
setup. The seven pairwise runs take about 500 s of wall clock and write about
74 MB of logs.

With three executors, `HOST` also fires on rows with no slow host (02, 05,
07): a hot key or hot partition leaves one executor far above the median on
shuffle bytes. It is not a target there.

### Known misses

Ten of the 48 tags the rows were designed for do not fire on the committed
logs. Each is recorded in the entry's `known_misses` with its measured
reason, rather than tuned to clear its threshold by a hair. The figures below
were measured with sparkforensics built from its main branch, which counts
killed speculative attempts that end after their stage and reads `CSTOR` from
block updates, and hold for 0.2.4 as well.

| Row | Tag | Why |
|---|---|---|
| 01, 02 | `CSTOR` | the whole persisted fact table stays cached (20 of 20 partitions); `CSTOR` flags some but under 90% |
| 04, 07 | `CSTOR` | no partition of the persisted fact table fits in storage memory at `spark.memory.fraction=0.02`, so none is cached |
| 02 | `SKEW` | join stage P95/median of 2.5 to 2.88 against a floor of 3; the hot key is 1 task of 2000, below P95 |
| 03 | `SPEC`, `HOST` | the slow host's tail attempts are speculated and killed as the application ends, and those killed attempts are never written, so the slow executor completes no task |
| 05 | `SPEC` | with no slow host the only slow task is the hot key's: one speculative attempt per run, against a floor of 5 losers and 60 s |
| 05 | `SKEW` | fires only from executor warm-up in the first map stage, never from the hot key: 1 of 3 runs |
| 07 | `SPEC` | no speculative attempt launched in 3 runs: with no slow host and no skew, no task lags far enough behind to be copied |

Some fired targets are timing-dependent as well. Across three Docker runs of
the final configuration, `SPEC` on 01, `HOST` on 04 (the slow executor did
not register in one run) and `SKEW` on 06 each missed once, and pairwise-07
once lost an executor to an out-of-memory error while merging its spill
files. A regeneration can land on a different set, so rerun
`scripts/verify_detectors.py` after one and update `targets_detectors` and
`known_misses` to what the new logs show.

## The failure scenarios

Four more runs sit outside the pairwise matrix, so they leave it untouched.
Each is the baseline config plus one `failure` mode, which replaces the join
workload with a small job that fails on purpose (`FAILURE_SCENARIOS` in
`src/corpus/matrix.py`, implemented in `workload/generate_events.py`):

| Run | What happens | Targets |
|---|---|---|
| `failure-task-retry` | every task fails its first attempt and succeeds on the retry | `RETRY` |
| `failure-stage-abort` | 4 of a stage's 20 tasks fail on every attempt, aborting the stage | `FAIL`, `SFAIL` |
| `failure-job` | one job of three fails; the driver catches it and ends cleanly | `JOBS` |
| `failure-killed-run` | the driver JVM halts after one job, so the log has no application-end event | `INCMP` |

Which attempts fail depends only on the partition index and the task attempt
number, never on timing, so every run fails the same way. The workload raises
if an injected failure does not fail its job, and `run_generation.py` expects
exit code 137 from the killed run and 0 from every other, so a scenario that
stops failing as designed breaks generation instead of producing a quietly
wrong log.

## The cache scenarios

Two more standalone runs give the cache-storage detector (`CSTOR`) logs that
record where each cached partition was stored (`CACHE_SCENARIOS` in
`src/corpus/matrix.py`). Each is the baseline config with the fact table
persisted, read by a second action after the join (`--second-action reread`),
under `storage_pressure`: the `STORAGE_PRESSURE_CONFS` in
`src/corpus/orchestration.py` shrink unified memory so the table no longer
fits, and set `spark.eventLog.logBlockUpdates.enabled=true`. Without that flag
the log has no `SparkListenerBlockUpdated` events, and the cache fields of
`RDD Info` that modern Spark writes are always 0, so no other log in the corpus
says what was cached.

| Run | Storage level | What happens |
|---|---|---|
| `cache-memory-only` | `MEMORY_ONLY` | partitions that do not fit are dropped, so only some stay cached |
| `cache-memory-and-disk` | `MEMORY_AND_DISK` | partitions that do not fit are written to disk instead |
