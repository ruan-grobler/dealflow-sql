# Performance case studies

Six before-and-after studies on a **1,165,043 row partitioned fact**, every number
measured on the live database and captured by [`benchmark.py`](benchmark.py).

The SQL lives in [`sql/06_performance.sql`](sql/06_performance.sql) and nowhere else.
`benchmark.py` parses the `-- @block` directives out of that file and executes them, so
the harness and the SQL cannot drift apart: there is exactly one copy of every query and
every index. Raw output of the run quoted here is in
[`plans/benchmark_run.txt`](plans/benchmark_run.txt), and every `EXPLAIN (ANALYZE, BUFFERS)`
plan is in [`plans/`](plans/).

Reproduce with `./run.sh benchmark`.

---

## Results

| Case | What was wrong | Before | After | Change |
|---|---|---:|---:|---:|
| 1 | Non-sargable month filter defeats partition pruning | 362.8 ms | 22.3 ms | **16.3x** |
| 2 | A broker performance review has no access path | 99.9 ms | 4.1 ms | **24.5x** |
| 3 | Dwell percentiles sort 1.14M rows with no ordered path | 6,967.6 ms | 5,588.4 ms | 1.2x |
| 4 | Open pipeline reads every deal to report on 4% of them | 45.2 ms | 7.8 ms | **5.8x** |
| 5 | BRIN on the partitioned fact | 138.9 ms | 29.7 ms | 4.7x |
| 5b | The free rewrite, head to head against that BRIN | 29.6 ms | 5.8 ms | **5.1x** |

Median of 9 timed runs after one warmup, client-observed round trip inside one open
session, so **planning time is included**. That is deliberate: planning time is part of
what a user waits for, and case 1 turns out to be as much a planning win as an execution
win.

### Measurement honesty

`EXPLAIN (ANALYZE, BUFFERS)` is captured **separately** from the timings, for plan shape
and buffer counts only. Its per-node instrumentation is not free: on an 85 partition plan
it inflated one measured query from 6.9 s to 24.3 s, roughly 3.5x. **Shape from EXPLAIN,
speed from the clock.**

The harness reports min / median / max for every case and counts competing backends
during each run. On the run quoted above, one spread was flagged: case 4's *before* had a
606 ms outlier against a 35.8 ms minimum. The medians held across four independent runs of
the whole suite, but this is a laptop container and the honest statement is that the
magnitudes are stable while the exact figures move by 20 to 40 percent between runs. Case 3
is the extreme: one run of the suite reported it *slower* after the fix. See below.

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

---

## Case 1. Non-sargable predicate, 16.3x

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
| Planning time | 6.902 ms | 0.373 ms |
| Buffers | 21,220 read | 1,098 hit, 0 read |

**Fix.** A half-open range on the partition key itself. No DDL, no index, no new object.

```sql
WHERE e.event_date >= DATE '2026-01-01'
  AND e.event_date <  DATE '2026-04-01'
```

**Why half-open and not `BETWEEN`.** `BETWEEN` on a timestamp silently drops or double
counts the boundary day, depending on whether the upper bound carries a time component.
Half-open ranges compose: the next quarter starts exactly where this one ends.

**Cost of partitioning, stated.** The planner must consider every partition before it can
prune, so planning time rises with partition count. Measured on this fact: **up to 21 ms
when no pruning applies against 0.2 ms when it does**
([`plans/evidence_710_pruning_plan_time.txt`](plans/evidence_710_pruning_plan_time.txt)).
That is why the partition count is capped in the low hundreds and why daily partitioning,
which would mean about 2,900 of them, was rejected.

---

## Case 2. Missing access path, 24.5x

**Question.** For one broker, across their whole history, how many stage events and what
value?

**Diagnosis.** No index leads with `broker_sk`, so all 85 partitions are sequentially
scanned and 1.16 million rows are filtered down to a few thousand.

**Fix.** `CREATE INDEX ix_fct_event_broker_date ON ... (broker_sk, event_date)`.

| | before | after |
|---|---:|---:|
| Buffers | 21,323 | **160** |
| Execution time in the plan | 149.2 ms | 1.2 ms |

**Why that column order.** `broker_sk` is the equality predicate so it must lead;
`event_date` is the range. A composite btree can only use a range predicate on the column
*after* the last equality. Reversed, the index would be a worse copy of partition pruning.

---

## Case 3. The case that only partly worked, 1.2x

This is the most useful study in the file, because the obvious fix mostly did not pay off
and the report says so.

**Question.** Across all history, what is the median and 90th percentile number of days a
deal spends in each stage?

**Diagnosis.** Two separate sorts, both spilling to disk. The `LAG` window needs its input
ordered by `(deal_nk, event_ts)` and nothing provides that order, so the planner sorts all
1.16 million rows: `Sort Method: external merge Disk: 47984kB`. `percentile_cont` then needs
its own sort of the same rows by dwell value: another `external merge Disk: 28448kB`.

**Fix.** A btree on `(deal_nk, event_ts)`. `Merge Append` reads the 85 partitions in key
order, so the window receives a pre-sorted stream and its 48 MB sort **disappears
entirely**.

| | before | after |
|---|---:|---:|
| Temp blocks written | 20,713 | **8,706** |
| Window step sort | external merge, 47,984 kB | gone, fed by `Merge Append` |
| Percentile sort | external merge, 28,448 kB | external merge, 28,448 kB |
| Median wall clock | 6,967.6 ms | 5,588.4 ms |

**The honest conclusion.** The plan improvement is unambiguous and the clock improvement is
marginal. `percentile_cont` has to sort its whole input to find a percentile; that is
inherent to the aggregate and no index can remove it. Worse, `Merge Append` over an index
gives up the parallel sequential scan and trades it for random heap access, and on one of
four suite runs that trade came out **net slower**. A report that quoted only the plan, or
only the best run, would be misleading.

**What actually fixes this question:** not an index. Materialise the aggregate. That is why
[`mart.mv_broker_month`](sql/04_facts.sql) exists.

---

## Case 4. Partial index and index-only scan, 5.8x

**Question.** Which open deals in late-funnel stages carry the most value at risk?

**Diagnosis.** Open deals are about 4 percent of a table that accumulates forever, and the
query read all 248,940 rows to report on them.

**Fix.** A partial index on the open subset, with the returned columns in `INCLUDE`.

| | before | after |
|---|---:|---:|
| Buffers | 33,965 | **202** |
| Heap fetches | n/a | **0** (index-only scan) |
| Index size | 12 MB (full equivalent) | **728 kB** (partial) |

Two separate wins, worth separating because they are different techniques:

- **`WHERE is_open`** makes the index track the working set instead of growing with
  history. Measured 16.9x smaller than the full-column equivalent
  ([`plans/evidence_820_partial_index_size.txt`](plans/evidence_820_partial_index_size.txt)).
- **`INCLUDE`** puts the returned-but-not-searched columns in the leaf pages, so the query
  never touches the heap at all. `Heap Fetches: 0` is the proof, and it only holds while
  the visibility map is current, which is why this table is `ANALYZE`d at the end of every
  load.

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

About **1,060x smaller**, and the planner chose it (`Bitmap Index Scan on
ix_raw_ingested_brin`). Evidence:
[`plans/evidence_810_brin_on_raw.txt`](plans/evidence_810_brin_on_raw.txt). This is why
`sql/01_staging.sql` puts BRIN on all four landing tables and nothing else.

**On the partitioned fact, BRIN was built, measured and deleted.** It does help a
timestamp-range query (138.9 ms to 29.7 ms), but case 5b puts it head to head against the
*free* rewrite that filters on the partition key instead:

| | Median | Partitions in plan | Buffers |
|---|---:|---:|---:|
| With the BRIN index | 29.6 ms | 85 | 665 |
| Rewrite on the partition key, no index | **5.8 ms** | **1** | **432** |

A further **5.1x faster with no new object to build, maintain or vacuum**. The arithmetic
explains it: the average partition is around 170 pages, so at the default 128
`pages_per_range` a partition holds one or two BRIN ranges and there is nothing to exclude.
85 BRIN indexes across the fact cost 2,040 kB
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
loop. Measured on this fact: an object on the **parent** was accepted, populated by
`ANALYZE`, and left the per-partition estimates **byte identical** (51, 96 and 244 rows
against actuals of 419, 924 and 1,779). The same object on the **child** corrected 96 to
exactly 924 against an actual 924. `util.fn_ensure_month_partition` therefore creates one
per partition as it creates the partition, and `DQ-008` fails the load if a populated
partition is missing one.

**`ADD CONSTRAINT ... NOT VALID` does not work on a partitioned parent.** The standard
low-lock pattern for adding a foreign key to a large table is `NOT VALID` followed by
`VALIDATE CONSTRAINT`. PostgreSQL 16 refuses it on a partitioned parent:

```
ERROR:  cannot add NOT VALID foreign key on partitioned table "fct_deal_stage_event"
        referencing relation "dim_property"
DETAIL:  This feature is not yet supported on partitioned tables.
```

`NOT VALID` does work on an individual leaf partition, which is a plain table. But the plain
validating `ADD CONSTRAINT` across all 85 partitions and 1,165,043 rows was measured at
**3,293 ms** for one key, so the workaround buys a shorter lock on each of 85 tables in
exchange for 85 separate DDL statements and no constraint on the parent. Three seconds for
referential integrity over the whole fact is worth taking. Worth knowing before a
maintenance window, not during one.
