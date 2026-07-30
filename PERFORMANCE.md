# Performance case studies

Six before-and-after studies on a **1,165,043 row partitioned fact**, every number
measured on the live database and captured by [`benchmark.py`](benchmark.py).

The SQL lives in [`sql/06_performance.sql`](sql/06_performance.sql) and nowhere else.
`benchmark.py` parses the `-- @block` directives out of that file and executes them, so
the harness and the SQL cannot drift apart: there is exactly one copy of every query and
every index. Raw output of the three runs quoted below is in
[`plans/benchmark_run.txt`](plans/benchmark_run.txt), and every `EXPLAIN (ANALYZE, BUFFERS)`
plan is in [`plans/`](plans/).

Reproduce with `./run.sh benchmark`. It is safe to run repeatedly: the fixture is gated by
a source-checksum provenance check, so a second run either reuses a fixture it has proved
matches the warehouse or rebuilds it from scratch.

---

## Results

Ranges are across **five runs of the whole suite**, each the median of 5 timed runs after one
warmup. Raw output of every run is in
[`plans/benchmark_run.txt`](plans/benchmark_run.txt).

| Case | What was wrong | Before | After | Speedup, 5 runs | Quote this |
|---|---|---:|---:|---:|---:|
| 1 | Non-sargable month filter defeats partition pruning | 220 to 412 ms | 12 to 52 ms | 7.7x to 24x | **~8x** |
| 2 | A broker performance review has no access path | 61 to 116 ms | 3.1 to 6.5 ms | 13x to 23x | **~13x** |
| 3 | Dwell percentiles sort 1.17M rows with no ordered path | 5.8 to 8.4 s | 3.5 to 6.2 s | 1.3x to 1.7x | **~1.3x** |
| 4 | Open pipeline reads every deal to report on 4% of them | 21 to 62 ms | 4.9 to 8.6 ms | 3.4x to 9.7x | **~3.4x** |
| 5 | BRIN on the partitioned fact | 72 to 280 ms | 13 to 25 ms | 5.0x to 12x | **~5x** |
| 5b | The free rewrite, head to head against that BRIN | 16 to 33 ms | 3.2 to 5.6 ms | 4.2x to 9.1x | **~4x** |

The last column is the **lowest speedup seen on any of the five runs**, and it is the number to
quote. A range whose top end is three times its bottom end does not support a decimal place, so
none is given.

### Why the ranges are that wide, and what is actually stable

Three of those five were **three consecutive runs with no manual reset between them**, which is
the re-runnability check. One was taken straight after a full `./run.sh`, which is the run a
reviewer following the README will actually get: rebuilding the warehouse pushes the lab's 166
MB of pages out of a 128 MB `shared_buffers`, so it starts cold. Per-run speedups:

| Case | Run 1 (rebuild) | Run 2 | Run 3 | Run 4 (cold) | Run 5 |
|---|---:|---:|---:|---:|---:|
| 1 | 7.7x | 24.0x | 16.9x | 17.4x | 11.2x |
| 2 | 22.7x | 20.3x | 15.5x | 13.5x | 15.0x |
| 3 | 1.7x | 1.6x | 1.6x | 1.3x | 1.5x |
| 4 | 4.0x | 3.4x | 4.5x | 4.2x | 9.7x |
| 5 | 5.8x | 5.6x | 5.0x | **11.7x** | 6.9x |
| 5b | 7.4x | 5.7x | 5.0x | 4.2x | 9.1x |

**Case 5 more than doubling on the cold run is the one to sit with.** Nothing about the index
improved. The un-indexed baseline it is measured against got worse, because a full scan suffers
more from a cold cache than an index probe does. That is exactly how a benchmark flatters a fix,
and it is why the quoted column is the minimum rather than the best. Case 4 doing the same thing
on run 5 makes the same point twice.

Timings are client-observed round trip inside one already open session, so **planning time
is included**. That is deliberate: planning time is part of what a user waits for, and case
1 turns out to be as much a planning win as an execution win.

### Measurement honesty

**The plan facts reproduce. The wall clock does not.** This is the single most useful thing in
this document. Over the same five runs whose speedups moved by 3x, these came back **byte
identical every single time**:

| Evidence | All five runs |
|---|---:|
| Case 1 fact partitions in the plan, before / after | 85 / 3 |
| Case 4 buffers, before / after | 33,965 / 202 |
| Case 3 temp blocks written, before / after | 20,713 / 8,706 |
| Case 5b buffers, BRIN / rewrite | 665 / 432 |

Where a buffer count did move, it moved between exactly **two** states, and which one you get
depends on whether that run rebuilt the fixture. Case 1's *after* reads 1,098 buffers with no
temp on the three runs that reused the fixture, and 1,046 buffers plus a 246 block temp spill on
the two that rebuilt it: a freshly built table has slightly different statistics, and the sort in
that query sits close enough to the 4 MB `work_mem` boundary to tip over it. Two reproducible
states is still reproducible. It is the milliseconds that wander.

`EXPLAIN (ANALYZE, BUFFERS)` is captured **separately** from the timings, for plan shape and
buffer counts only, because its per-node instrumentation is not free. Measured on this build it
is modest: the largest gap was case 4's *after* query at 4.9 ms on the clock against 7.2 ms of
planning plus execution self-reported by `EXPLAIN ANALYZE`, about 1.5x, and case 3's *after*
query at 3,520 ms against 4,616 ms, about 1.3x. **Shape from EXPLAIN, speed from the clock**, and
the reason is the direction of the bias rather than its size.

The harness reports min / median / max for every case and counts competing backends during each
run. Across all five runs it flagged nothing: no competing backends and every min-to-max spread
within tolerance. An earlier run of the suite, taken while a warehouse load was running in the
same container, reported case 3's *before* at 12,173 ms against 2,830 ms on a quiet box, which is
why that check exists at all.

### Container under test

PostgreSQL 16.14 on aarch64, 8 vCPU, `shared_buffers=128MB`, `work_mem=4MB`,
`max_parallel_workers_per_gather=2`. The small `work_mem` is the container default and is
left alone on purpose: it is what makes the disk spill in case 3 a real observation rather
than a manufactured one.

The lab lives in its own `perf` schema, built **from `mart`** and verified against it at
run time (same row count, same value distribution, same physical ordering). Measuring a
*before* state means dropping the exact indexes the warehouse depends on, and doing that to
a loaded `mart` would mutate a production object mid-flight and reset the index usage
counters that `mart`'s own index justification artifact reads.

`perf.fct_deal_pipeline` holds **248,702** rows against `mart.fct_deal_pipeline`'s 248,940,
and the difference is not a defect: the lab's snapshot is aggregated from stage events, so
the 238 deals that carry no stage event have no row in it.

---

## Case 1. Non-sargable predicate, about 8x

**Question.** For the first quarter of 2026, how many deals moved between each pair of
stages, and how long did they sit in the previous stage?

**Before.**

```sql
WHERE to_char(e.event_ts, 'YYYY-MM') IN ('2026-01', '2026-02', '2026-03')
```

**Diagnosis.** The filter wraps the timestamp in `to_char()` and compares the result to
text. The planner cannot reason about a function result against partition boundaries, so it
prunes nothing:

| | before | after |
|---|---:|---:|
| Fact partitions in the plan | **85** | **3** |
| Planning time | 2.1 to 6.7 ms | 0.23 to 0.31 ms |
| Buffers | 21,220 read | 1,098 hit, 0 read |

Partition count and buffers were byte identical on all three runs. Planning time is given as
the range because it is the figure that moved.

**Fix.** A half-open range on the partition key itself. No DDL, no index, no new object.

```sql
WHERE e.event_date >= DATE '2026-01-01'
  AND e.event_date <  DATE '2026-04-01'
```

**Why half-open and not `BETWEEN`.** `BETWEEN` on a timestamp silently drops or double
counts the boundary day, depending on whether the upper bound carries a time component.
Half-open ranges compose: the next quarter starts exactly where this one ends.

**Cost of partitioning, stated.** The planner must consider every partition before it can
prune, so planning time rises with partition count. Measured on this fact: **0.23 to 1.25 ms
when pruning applies at plan time, 7.6 ms when nothing prunes, and 13.8 ms for a generic
parameterised plan that carries all 85 partitions and eliminates 82 of them during execution**
(`Subplans Removed: 82`), from
[`evidence_710`](plans/evidence_710_pruning_plan_time.txt),
[`evidence_730`](plans/evidence_730_no_pruning.txt) and
[`evidence_720`](plans/evidence_720_pruning_run_time.txt). Roughly an order of magnitude of
planning time, paid on every query, is why the partition count is capped in the low hundreds
and why daily partitioning, which would mean about 2,900 of them, was rejected.

---

## Case 2. Missing access path, about 16x

**Question.** For one broker, across their whole history, how many stage events and what
value?

**Diagnosis.** No index leads with `broker_sk`, so all 85 partitions are sequentially
scanned and 1,165,043 rows are filtered down to the **114** that belong to broker 407, which
is 0.0098 percent of the fact.

**Fix.** `CREATE INDEX ix_fct_event_broker_date ON ... (broker_sk, event_date)`.

| | before | after |
|---|---:|---:|
| Buffers | 21,351, and 21,579 on the cold run | **320**, and 615 on the cold run |
| Execution time in the plan | 61 to 536 ms | 1.8 to 2.4 ms |

Even the cold-cache figure is a 35x reduction in buffers, and the two warm runs agree exactly.
Note also that the *after* plan still carries 84 partitions: the index is created on the
partitioned parent, so each partition contributes its own index scan, but each one now costs a
handful of buffers instead of a full scan. Indexing and pruning are solving different problems
here, which is the point of the case.

**Why that column order.** `broker_sk` is the equality predicate so it must lead;
`event_date` is the range. A composite btree can only use a range predicate on the column
*after* the last equality. Reversed, the index would be a worse copy of partition pruning.

---

## Case 3. The case that only partly worked, about 1.6x

This is the most useful study in the file, because the obvious fix mostly did not pay off
and the report says so.

**Question.** Across all history, what is the median and 90th percentile number of days a
deal spends in each stage?

**Diagnosis.** Two separate sorts, both spilling to disk. The `LAG` window needs its input
ordered by `(deal_nk, event_ts)` and nothing provides that order, so the planner sorts all
1,165,043 rows: `Sort Method: external merge Disk: 47984kB`. `percentile_cont` then needs
its own sort of the same rows by dwell value: another `external merge Disk: 28448kB`.

**Fix.** A btree on `(deal_nk, event_ts)`. `Merge Append` reads the 85 partitions in key
order, so the window receives a pre-sorted stream and its 48 MB sort **disappears
entirely**.

| | before | after |
|---|---:|---:|
| Temp blocks written | 20,713 | **8,706** |
| Window step sort | external merge, 47,984 kB | gone, fed by `Merge Append` |
| Percentile sort | external merge, 28,448 kB | external merge, 28,448 kB |
| Median wall clock | 5,789 to 6,038 ms | 3,520 to 3,637 ms |

Every row of that table except the last was byte identical on all three runs.

**The honest conclusion.** The plan improvement is unambiguous and the clock improvement is
real but small: 1.6x to 1.7x, against a 2.4x reduction in temp blocks written.
`percentile_cont` has to sort its whole input to find a percentile; that is inherent to the
aggregate and no index can remove it, so the 28 MB spill stays. Worse, `Merge Append` over an
index gives up the parallel sequential scan and trades it for random heap access: the *after*
plan touches **1,127,596** shared buffers (1,099,256 hits plus 28,340 reads) against the
*before* plan's 21,221, and it wins anyway only because almost all of that is a cache hit on a
166 MB fact inside a container that has more memory than that. On a box with less memory the
trade could invert,
and an earlier run on a loaded laptop did report this case **net slower**. It did not recur
in the three runs recorded here. A report that quoted only the plan, or only its best run,
would be misleading.

**What actually fixes this question:** not an index. Materialise the aggregate. That is why
[`mart.mv_broker_month`](sql/04_facts.sql) exists.

---

## Case 4. Partial index and index-only scan, about 3.4x

**Question.** Which open deals in late-funnel stages carry the most value at risk?

**Diagnosis.** The query reads all **248,702** rows of a table that accumulates forever to
report on the **9,838** that are both open and sitting in stages 3 to 7, which is 4.0 percent
of it. (Open deals alone are 14,694, or 5.9 percent; the stage filter takes it to 4.0.) The
plan shows `Rows Removed by Filter: 79,621` per worker.

**Fix.** A partial index on the open subset, with the returned columns in `INCLUDE`.

| | before | after |
|---|---:|---:|
| Buffers | 33,965 | **202** |
| Heap fetches | n/a | **0** (index-only scan) |
| Index size | 12 MB (full equivalent) | **728 kB** (partial) |

Two separate wins, worth separating because they are different techniques:

- **`WHERE is_open`** makes the index track the working set instead of growing with
  history. Measured **16.5x** smaller than the full-column equivalent, 745,472 bytes against
  12,320,768
  ([`plans/evidence_820_partial_index_size.txt`](plans/evidence_820_partial_index_size.txt)).
- **`INCLUDE`** puts the returned-but-not-searched columns in the leaf pages, so the query
  never touches the heap at all. `Heap Fetches: 0` is the proof, and it is conditional: an
  index-only scan still has to consult the heap for any page not marked all-visible in the
  **visibility map**, and that map is maintained by `VACUUM`, not by `ANALYZE`. On this fixture
  autovacuum does it after the bulk load, and `pg_class` confirms it: `relallvisible` 4,431 of
  `relpages` 4,431. **So a freshly loaded table can report non-zero heap fetches until vacuum
  catches up**, and the index is not the thing that changed. Worth knowing before blaming an
  index for a plan that looked better yesterday.

---

## Case 5. BRIN, measured and then rejected

BRIN stores one min/max summary per block range instead of one entry per row. That is
enormously cheaper, but only useful when the indexed column correlates with physical row
position.

**On the unpartitioned landing table, BRIN is the right answer:**

| Index on `raw.stage_event(ingested_at)` | Size |
|---|---:|
| BRIN | **24 kB** |
| btree | **25 MB** |

About **1,060x smaller** (26,157,056 bytes against 24,576), and the planner chose it
(`Bitmap Index Scan on ix_raw_ingested_brin`). Evidence:
[`plans/evidence_810_brin_on_raw.txt`](plans/evidence_810_brin_on_raw.txt). This is why
`sql/01_staging.sql` puts BRIN on all four landing tables and nothing else.

**On the partitioned fact, BRIN was built, measured and deleted.** It does help a
timestamp-range query (72 to 86 ms down to 13 to 16 ms), but case 5b puts it head to head
against the *free* rewrite that filters on the partition key instead:

| | Median | Partitions in plan | Buffers |
|---|---:|---:|---:|
| With the BRIN index | 16 to 24 ms | 85 | 665 |
| Rewrite on the partition key, no index | **3.2 to 3.3 ms** | **1** | **432** |

A further **5x faster with no new object to build, maintain or vacuum**. The partition counts
and both buffer figures were identical on all three runs. The arithmetic explains it: the
average partition is about 250 pages, so at the default 128 `pages_per_range` a partition
holds two BRIN ranges and there is almost nothing a summary can exclude. 85 BRIN indexes
across the fact cost 2,040 kB
([`plans/evidence_830_brin_size_on_fact.txt`](plans/evidence_830_brin_size_on_fact.txt))
to be slower than free.

**The transferable lesson:** partition pruning and BRIN solve the same problem, and if you
already have declarative partitioning on the column, BRIN on that column is redundant. Fix
the query before you add an index.

---

## Two more measured findings

**Extended statistics belong on the partition, never on the parent.** `from_stage_sk` and
`to_stage_sk` are near functionally dependent (deals move forward one stage at a time), so
without extended statistics the planner multiplies two independent selectivities and badly
underestimates every funnel query. A bad row estimate is what flips a hash join to a nested
loop.

The clean A/B is already sitting in this database, because `mart`'s partitions carry a
statistics object and the `perf` mirror's partitions carry none. Same data, same partition,
same predicate (`from_stage_sk = 1 AND to_stage_sk = 2` on the February 2026 partition,
actual 4,497 rows):

| | Estimated rows | Actual |
|---|---:|---:|
| No extended statistics (`perf` partition) | 1,136 | 4,497 |
| Object on the **parent**, `ANALYZE` on the parent | 1,136 | 4,497 |
| Object on the **partition** (`mart`) | **4,497** | 4,497 |

Without the object the planner is out by 4.0x. An object on the parent, accepted by
PostgreSQL and populated by `ANALYZE`, changes the per-partition estimate by **nothing at
all**: 1,136 before and 1,136 after, because the planner estimates each partition from that
partition's own statistics. The per-partition object lands the estimate exactly, and it needs
all three kinds to do so, `(ndistinct, dependencies, mcv)`, which is what
`util.fn_ensure_month_partition` creates. Recreating it with only `(ndistinct, dependencies)`
got to 2,094: better than 1,136, still out by 2.1x, and `mcv` is what closes the gap. `DQ-008`
fails the load if a populated partition is missing its object, and `ASSERT-WH-029` reports the
same thing in the assertion battery.

**`ADD CONSTRAINT ... NOT VALID` does not work on a partitioned parent.** The standard
low-lock pattern for adding a foreign key to a large table is `NOT VALID` followed by
`VALIDATE CONSTRAINT`. PostgreSQL 16 refuses it on a partitioned parent:

```
ERROR:  cannot add NOT VALID foreign key on partitioned table "fct_deal_stage_event"
        referencing relation "dim_property"
DETAIL:  This feature is not yet supported on partitioned tables.
```

`NOT VALID` does work on an individual leaf partition, which is a plain table: measured **3.3
ms to add plus 1.9 ms to validate** on the 19,406 row February 2026 partition. But the plain
validating `ADD CONSTRAINT` on the parent, across all 85 partitions and 1,165,043 rows, is
cheap enough that the workaround is not worth it: **117 to 344 ms for one key** over four
samples (the 344 ms was the first, on a cold cache), and **1.8 to 2.9 s for all five keys
together**, which is what the fixture's `100_fact_foreign_keys` block reports on every run.

So the per-partition workaround would buy a shorter lock on each of 85 tables in exchange for
85 separate DDL statements, no constraint on the parent, and nothing to show for it at this
volume. Under two seconds of exclusive lock for referential integrity across the whole fact is
worth taking. The figure to carry forward is the shape of it: this cost grows with row count,
so measure it at your volume before a maintenance window, not during one.
