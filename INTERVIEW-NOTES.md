# Interview notes

This file is for me, not for a reviewer. Every technique in the repository gets three
things: **what to say out loud** in plain English, **the question that follows**, and **the
honest answer**, including where my experience runs out. If I cannot say the plain-English
version without reading it, I do not understand it well enough to claim it.

**The rule I am holding myself to:** never claim production experience I do not have. The
true and strong sentence is *"I built this to production shape and measured everything in
it. I have not operated it at scale."* That is a good answer. Bluffing is not, and a senior
engineer spots it in one follow-up question.

**If I only remember three things:**

1. Every number in the README came off the database. If asked where one came from, I can
   name the file that produced it.
2. Two things in here are wrong-then-fixed, and I should volunteer both, because they are
   the most convincing parts: the dedupe key that caught less than half the duplicates, and
   the single-pass anomaly removal that my own assertion caught being unsafe.
3. "I do not know, here is how I would find out" is a passing answer. Guessing is not.

---

## 1. Star schema, two facts at two grains

**Say it.** The warehouse has two fact tables because the business asks two different
shapes of question. One is about transitions: how many deals went from Qualified to Offer
last quarter, and how long did they sit. The other is about deals: how long from receipt to
offer, by cohort. The first needs a row per stage change, the second needs a row per deal,
and they share the same dimensions so the two answers reconcile.

**They will ask: why not one wide table?** Because the grain would have to be one or the
other and the other question then gets expensive. And because a wide table breaks Type 2:
if agency attributes sat on the broker row, reclassifying one agency would mean rewriting
every historical broker version, which is the exact rewrite Type 2 exists to avoid.

**Honest limit.** I have designed one star. I have not lived through the argument where the
business wants a third grain and someone proposes bolting it onto an existing fact, which I
gather is where most of the real judgement happens.

---

## 2. Slowly Changing Dimension Type 2

**Say it.** A report about last year has to describe last year. If a broker moved agency in
March 2023, their 2022 deals still belong to the old agency. So instead of one row per
broker, I keep one row per broker per period during which their tracked attributes did not
change, each row stamped with `valid_from` and `valid_to`. When a fact is loaded I join on
"which version of this broker was valid at the moment this event happened", not "who are
they now".

**They will ask: how do you detect a change?** I hash the tracked attributes into
`row_hash` and compare. The broker feed is a full monthly snapshot, so a full snapshot tells
me the state and never the change; the hash is what turns state into change. The property
feed is the opposite, a change-only delta, and I use the identical code, because a delta feed
cannot be trusted either: a row that says "reclassified" but changed nothing tracked must not
open a version, and the hash is what proves it did not.

**They will ask: what is NOT in the hash?** Name, email and phone, on purpose. Those are
Type 1 and get overwritten across every version. A corrected spelling is not a new version
of a person, and if I hashed it, every typo fix would open a spurious version.

**They will ask: what goes wrong if you get this wrong?** Two things. Two versions covering
the same instant, so a fact joins to both and every total doubles. Or `is_current` drifting
out of step with the dates after a manual backfill, so "current brokers" returns the wrong
set. I stopped both at the schema level rather than in the loader: an `EXCLUDE` constraint
makes overlap impossible, and `is_current` is a generated column so it cannot be written by
hand at all.

**The `EXCLUDE` constraint, in plain English.** `EXCLUDE USING gist (broker_nk WITH =,
tstzrange(valid_from, valid_to) WITH &&)` says: no two rows may have the same broker AND
overlapping date ranges. `&&` is "overlaps". It needs the `btree_gist` extension because it
mixes a text equality test with a range overlap test in one index. One constraint gives me
two guarantees, because "exactly one current version" follows for free: two open versions
both end at infinity, so they always overlap, so the constraint rejects the second one.

**The one detail I would get asked about and should not fumble.** `valid_to` is
`NOT NULL DEFAULT 'infinity'`, never NULL. If it were NULL, every point-in-time join would
have to be written `(valid_to IS NULL OR event_ts < valid_to)`, and the day somebody forgets
the first half is the day the current version silently stops matching. With infinity it is
one half-open range test.

**Honest limit.** I have not had to load a Type 2 dimension where the source sends
retroactive corrections dated *before* the current version's `valid_from`. My loader closes
the current version and opens a new one going forward. A true late-arriving correction needs
splitting an existing version in two, and I have read about it but not built it.

---

## 3. Declarative range partitioning

**Say it.** The event fact is one logical table split into 84 physical monthly tables plus a
catch-all. When a query filters on a date range, PostgreSQL only reads the months that can
contain matching rows. In my case-1 study the query went from touching all 85 partitions to
3, and from 21,220 disk blocks read to 1,098 already in cache.

**They will ask: what is the downside?** The planner has to consider every partition before
it can prune, so planning time grows with partition count. I measured it: up to 21 ms when
no pruning applies against 0.2 ms when it does. That is why I went monthly and not daily.
Daily would be about 2,900 partitions, and the planning cost would eat the pruning win on
small queries.

**They will ask: what is the DEFAULT partition for?** An event with a mistyped year lands
there instead of aborting the load. There are 369 of them from a deliberately seeded defect.
A deliberate landing zone with an alarm on it beats a load that dies at 3am, and rule DQ-012
reports its depth every run with an expected band of 1 to 8,000 rather than asserting it
empty.

**They will ask: why is the primary key `(deal_event_sk, event_date)` and not just the
key?** Because PostgreSQL requires the partition key in every unique index on a partitioned
table. It is a storage constraint, not a modelling choice. That constraint is also why the
snapshot fact is NOT partitioned: `PRIMARY KEY (deal_nk)` would become illegal and the
enforced grain would silently become `(deal_nk, received_date)`. I proved it on the live
database. I restated one deal's received date, re-ran the `MERGE`, and it inserted a second
row, leaving the same deal alive in two partitions at once.

**Honest limit.** I have never detached or dropped a partition for retention in anger. I
built `meta.partition_registry` with a `detached_at` column and I know
`DETACH CONCURRENTLY` exists, but I have not run a retention job or dealt with the locking.

---

## 4. Extended statistics per partition

**Say it.** `from_stage_sk` and `to_stage_sk` are highly correlated, because deals mostly
move forward one stage at a time. Without telling the planner that, it treats them as
independent, multiplies two selectivities, and badly underestimates how many rows a funnel
query will return. A bad row estimate is what flips a hash join into a nested loop and turns
80 ms into minutes.

**They will ask: how do you know it worked?** I measured it, and the interesting part is that
the obvious placement failed. One predicate, `from_stage_sk = 1 AND to_stage_sk = 2` on the
February 2026 partition, actual 4,497 rows:

- No extended statistics: the planner estimates **1,136**, out by 4x.
- An object on the partitioned **parent**, accepted by PostgreSQL and populated by `ANALYZE`:
  still **1,136**. It changed nothing, because the planner estimates each partition from that
  partition's own statistics.
- The object on the **partition**: **4,497**, exact.

My partition-maker function therefore creates one per partition as it creates the partition,
and rule DQ-008 fails the load if a populated partition is missing one.

**Honest limit, and I can now put a number on it.** I used to say I was shaky on `mcv`. When I
re-ran this I built the partition-level object with only `(ndistinct, dependencies)` and got
2,094 against the actual 4,497: better than 1,136, still out by 2x. Adding `mcv` is what lands
it exactly, because the estimate then comes from a stored frequency for that specific pair
rather than from a correction to a product of selectivities. I still have not had to diagnose a
plan regression under time pressure.

---

## 5. BRIN, and why I deleted the one on the fact

**Say it.** BRIN stores one min/max summary per block range instead of one entry per row, so
it is tiny, but it only works when the column correlates with physical row position. On my
landing table, which is written in ingest order, BRIN on the ingest timestamp is 24 kB
against 25 MB for the equivalent btree, and the planner chose it. On the partitioned fact I
built one, measured it, and deleted it.

**They will ask: why delete it?** Two reasons and the second is the real one. First, the
average partition is about 250 pages, and at the default 128 pages per range a partition holds
two BRIN summaries, so there is almost nothing to exclude. Second, I put it head to head
against simply rewriting the query to filter on the partition key, and the rewrite was 5x to
7x faster than the BRIN version across three runs, with no object to build, maintain or
vacuum, and it read 432 buffers against 665 on every single run. Partition
pruning and BRIN solve the same problem, so if you already partition on the column, BRIN on
that column is redundant.

**The line I want to land.** Fix the query before you add an index.

**Honest limit.** I have not tuned `pages_per_range`. I left it at the default because 24 kB
was already small enough that a change needed a measurement to justify it, and I did not
have one.

---

## 6. Error-tolerant loading, and the reconciliation that makes it safe

**Say it.** One unparseable value must not cost the other 262,499 rows their load. So bad
rows are diverted to a reject table with a reason code and the original text, and the run
continues. The dangerous part of that design is that "we divert bad rows" and "we lose rows"
look identical from outside, so rule DQ-001 proves it: raw rows offered must exactly equal
staged plus rejected plus quarantined. On this data it is 262,500 equals 258,562 plus 3,938,
and 1,204,742 equals 1,180,447 plus 24,295. Zero difference.

**They will ask: why not `TRY_CAST`?** PostgreSQL does not have one, so the usual trick is a
plpgsql function with an exception handler. I have two of those and I do not use them on the
hot path, because a plpgsql `EXCEPTION` block opens a subtransaction and a parallel worker
cannot open one. Marked parallel safe it fails outright with *"cannot start subtransactions
during a parallel operation"*, and marked honestly as parallel unsafe it forces every query
that mentions it to run serially. So the two big feeds are classified with immutable,
parallel-safe regex predicates first, and only the rows that will cast are cast.

**They will ask: how do you catch `2026-02-30`?** A regex accepts it and so does a naive
length check, because it looks like a date. My `fn_is_real_date` reassembles the month's
actual length and compares, so an impossible day fails. This matters because `to_date` would
be worse than an error: it silently returns 2026-03-02, and the deal enters the warehouse
two days late with nothing ever complaining.

**Honest limit.** My reject tables have no re-drive path. In production somebody has to fix
those 3,938 rows and re-submit them, and I have not built or operated that loop.

---

## 7. Deduplication, and the version of it that was wrong

**Volunteer this one.** It is the most convincing thing in the repository.

**Say it.** Resubmissions arrive with a fresh message id, a fresh deal reference, a later
timestamp and a value jittered by up to 2 percent, so no identifier finds them. My first key
was broker plus property plus the value bucketed to the nearest R100,000 plus the ISO week.
It caught 4,588 of about 9,600. The reason is that the jitter is **proportional** and my
bucket was **absolute**: 2 percent of a R30 million deal is R600,000, which is six buckets
wide. A relative bucket does not fix it either, it just moves the problem to the bucket
boundaries, which the jitter straddles.

**What I changed.** The value came out of the key entirely and became evidence instead. The
key is now a blocking key, broker plus property plus ISO week, and the value delta is
recorded in the suppression log where a reviewer can audit the decision. A key has to be
exact; evidence does not. That took it to 9,622 suppressions, a rate of 3.7 percent, inside
the expected band.

**They will ask: what does that get wrong?** Two things, both measured. In 349 of 9,597
blocks the values differ by more than 5 percent, so some of those suppressions might be
genuinely distinct deals rather than resubmissions. And worse, blocking has a structural
blind spot: when the defect lands **on the blocking key itself**, for instance a mangled
broker code on one row of a pair, the two rows fall into different blocks and both survive.
That is 238 deals, just under 0.1 percent, and they show up as deals in the snapshot with no
event history.

**They will ask: so why not fix it?** I priced it rather than hid it. The fix is multi-pass
blocking: a second pass on property plus week catches the broker-mangled pairs, a third on
broker plus week catches the property-mangled ones, and then you need transitive clustering
across the passes, which in PostgreSQL means a recursive CTE over candidate pairs. That is
the right design where 0.1 percent matters. At this volume it is over-engineering, so instead
`ASSERT-WH-014` bands the residual at 400 and alarms if it drifts.

**They will ask: why not fuzzy matching?** For the property names I chose deterministic
normalisation over trigram similarity deliberately. `similarity()` returns a score, a score
needs a threshold, and a threshold is a number I cannot defend in a review. A normalisation
either matches or it does not, the same way every run, and the unmatched remainder lands on
the unknown member where a rule counts it.

**Honest limit.** I have not used `pg_trgm` in anger and I have never built a real record
linkage system. I know the vocabulary, blocking and comparison and transitive closure, from
solving this one problem.

---

## 8. Window functions

**Say it.** A transition only exists relative to the previous event for the same deal, so
`LAG` over (deal, event order) is the whole computation for both `from_stage` and time in
stage. I do it once at load time. If I left it to reports, every report would re-run a
window over 1.1 million rows, and two reports would eventually disagree because one of them
ordered the partition slightly differently.

**They will ask: what is the tiebreaker for?** Two events can share a timestamp. If the
order between them is not deterministic, every dwell measure downstream moves between runs
and nobody can reconcile yesterday's report against today's. So the order is
`(event_ts, raw_line_no)`, and `raw_line_no` is the row's address in the source file.

**They will ask about `ROWS` versus `RANGE`.** `ROWS` counts physical rows either side of the
current one. `RANGE` with an interval counts calendar distance. On a dense monthly series
they agree and the distinction looks academic. My Q04 series is deliberately not dense,
because a single broker closes nothing in many months, so those months have no row at all.
`ROWS BETWEEN 2 PRECEDING` happily reaches back across a six-month gap and calls the result
a three-month average. `RANGE BETWEEN INTERVAL '2 months' PRECEDING` only averages what
genuinely falls in the window. The query returns both frame sizes side by side so the
divergence is visible instead of asserted. Rule of thumb: a cumulative total wants `ROWS`,
any time-based average wants `RANGE`.

**They will ask about gaps and islands.** Q12 finds each broker's longest unbroken run of
active months. The trick is that `row_number()` increments by one every row, and a dense
month series also increments by one, so subtracting them gives a constant that is the same
for every month in an unbroken run and changes when there is a gap. Group by that constant
and each group is an island.

**A bug I found in my own analytics and should mention.** Q04 picks the top-earning broker
with `ORDER BY sum(value) DESC LIMIT 1`. PostgreSQL defaults `DESC` to `NULLS FIRST`, and
about 1.2 percent of submissions arrive with no value at all, so a broker whose only won deal
has a NULL value sums to NULL and sorts **above** the genuine top earner. The query returned
one row for that broker, which looked like an empty result rather than a wrong one, which is
exactly why it survived a read-through. `DESC NULLS LAST` fixes it. Any "top N by an
aggregate that can be NULL" needs it.

---

## 9. `MERGE` and idempotence

**Say it.** Running the load twice must leave the database exactly as running it once. That
is what makes a failed 3am load safe to retry instead of something to unpick by hand. I get
it four different ways depending on the table: `MERGE` on the source key for staging and the
snapshot fact, `INSERT ... ON CONFLICT DO NOTHING` on the raw row address for the reject and
quarantine tables, `TRUNCATE` and rebuild for the derived intermediate tables, and for the
event fact `ON CONFLICT` against the unique index on the source event id.

**They will ask: why `ON CONFLICT` on the fact and not an anti join?** Because it makes the
guarantee physical rather than procedural. An anti join has to be remembered by whoever
writes the next loader; a unique index cannot be forgotten. It is also cheaper: one index
probe per row during the insert, against a semi join that would probe all 85 partitions for
every one of 1.1 million rows before inserting any of them.

**They will ask: did you verify it?** Yes. I ran both loaders a second time against the
loaded warehouse and every row count came back byte identical, the anomaly loop reported
zero passes because there was nothing left to remove, and the Type 2 dimensions opened no
new versions because the hash compare found no change.

**They will ask: has your idempotence ever actually broken?** Yes, once, and in the benchmark
rather than the warehouse. The performance lab caches its fixture, because rebuilding 1.16
million rows takes 68 seconds and checking them takes 3. The bug was the key it cached on. It
asked "is this broker version already loaded?" by comparing `broker_sk`, which is a surrogate
key the lab does not own: my warehouse loader assigns it from an identity sequence with no
`ORDER BY`, so rebuilding the warehouse can hand the same brokers different numbers. When that
happened the guard saw keys it did not recognise, tried to insert versions it already held, and
the SCD Type 2 exclusion constraint stopped it:

```
ERROR:  conflicting key value violates exclusion constraint "ex_dim_broker_no_overlap"
```

The constraint was keyed on the row's real identity, `(broker_nk, validity window)`. The guard
was keyed on a number that had moved. **An idempotence check has to be keyed on the same thing
the constraint is keyed on**, otherwise the constraint is what tells you the check was wrong.

**They will ask: so you added `ON CONFLICT DO NOTHING`?** No, and this is the part I would want
to be asked about. An untargeted `ON CONFLICT DO NOTHING` does swallow exclusion constraint
violations, so it would have made the error go away. It would also have left the lab holding
rows keyed to the old numbers while every fact row resolved against the new ones, and case study
2 filters `WHERE broker_sk = 407`, so it would have gone on reporting a speedup for a different
broker. Trading a loud crash for a silent wrong measurement is the worst trade available. I
fixed both halves instead: the guard now matches on `(broker_nk, valid_from)`, and the fixture
records a checksum of every warehouse column it copied, so a source that has moved forces a
clean rebuild rather than a patch. Three consecutive runs with no manual reset, all clean.

**Honest limit.** `MERGE` arrived in PostgreSQL 15 and this is the first thing I have used it
on. I know it does not support `RETURNING` in 16, which is why the restatement log is written
as a separate statement before the `MERGE` rather than out of it. And the defect above shipped
in the first place, which is the honest version of this section: I did not catch it until I ran
the benchmark twice in a row.

---

## 10. Watermarks and the run log

**Say it.** Each source stream has a high water mark, and the load only reads rows newer than
it. They start at minus infinity, so the very first load and every nightly delta run through
exactly the same statements. A separate bootstrap script is the classic way for a warehouse
to grow two behaviours that drift apart, and the fix is to not have one.

**They will ask: when do the watermarks advance?** Last, and only after the final quality
gate has passed. A watermark that advances before the gate turns a failed run into
permanently skipped data, and nobody finds out until a month-end number is wrong. Because
every write is idempotent, a failed run can just reprocess the same batch, which is what
earns the right to do it in that order.

---

## 11. Quality rules as data, and the assertion suite

**Say it.** Twelve rules live in a table, each with its SQL, a threshold band, and the name
of the defect it exists to catch. One function runs all of them, records every observation
whether it passed or failed, and only then raises if an error-severity rule failed.
Recording before raising is deliberate: an engineer debugging a failed run needs all twelve
observations, not just the first failure. And because results are append-only, quality is a
trend line rather than a one-off assertion.

**They will ask: which rule matters most?** DQ-005. A foreign key proves the surrogate key
exists. Only DQ-005 proves it was the version **in force when the event happened**, across
all 1,165,043 fact rows. That is the one thing about a Type 2 dimension that no constraint
can check for you.

**They will ask why some rules expect a non-zero answer.** Because the data deliberately
contains defects, and a rule that asserts zero on data that is designed to be non-zero
would fail every run and get switched off. The duplicate rate is banded 2 to 7 percent. The
default partition depth is banded 1 to 8,000, and the band starting at 1 is the point: a zero
would mean the generator stopped producing the defect and the rule stopped being tested.

**They will ask about the 51 assertions.** Two batteries with different jobs. The
**warehouse** battery is my own correctness contract, 30 assertions, all of which must pass,
and all 30 do. The **staging** battery is a supplier scorecard, and it reports 18 findings on
purpose: those are the evidence for a conversation with the source system owner, not a build
failure. The runner exits non-zero on a blocking failure so it can gate a pipeline, and the
build script deliberately tolerates the staging exit code and gates on the warehouse one.

**Something a reviewer might push on, and they would be right to.** I changed one assertion
from a zero-tolerance error to a banded warn. That looks like moving the goalposts, so I
should say why before they ask: the original expectation was testing an invariant the design
never claimed. The measured population is understood and bounded, the reasoning is written
into the assertion's own comment, and the band is what catches a regression.

---

## 12. Convergent anomaly removal

**Volunteer this one too.**

**Say it.** Some seeded events are physically impossible, like a transition out of a closed
stage. I detect those after deriving the previous stage with `LAG`, then quarantine them. My
first version did it in one pass, on the reasoning that every anomaly is the last event of
its deal so removing it cannot affect anything else. I did not trust the reasoning, so I
wrote an assertion to check it, and **the assertion failed at 1,219 rows.**

**What was actually happening.** A second, independent defect. A mistyped year throws an
event to 2031, so it sorts after its deal's real terminal event, which makes the transition
into it come out of a terminal stage. Removing an anomaly in the middle of a deal then
changes the previous stage of the event after it, which can create a brand new anomaly that
was invisible on the first pass.

**The fix.** A loop that re-derives only the affected deals and repeats until nothing
changes. It converges in two passes on this data, 8,155 events then 1,139. There is a hard
iteration cap that raises a clear error, because each pass must remove at least one event, so
if that argument ever stops holding I want the load to fail in seconds rather than spin all
night.

**The line I want to land.** This is the difference between a pipeline that is correct and
one that happens to be correct on the data the author looked at.

---

## 13. Deterministic synthetic data

**Say it.** The dataset has to be byte identical on every machine, or none of the
measurements are reproducible. `setseed()` plus `random()` does not achieve that, and I
measured it: two identical runs gave different checksums, because every parallel worker seeds
its own generator, and any query over 250,000 rows goes parallel on this box. So every
pseudo-random decision comes from a hash of the row's own natural key instead. That draw does
not depend on plan shape, worker count or row order, and I verified identical checksums under
a forced parallel plan and under serial execution.

---

## 14. Materialized view, dense on purpose

**Say it.** `mv_broker_month` is one row per current broker per month, and critically it is
dense. A plain `GROUP BY` over the fact only returns broker-months that had activity, so feed
that to a moving average or a month-over-month delta and the gaps close up silently: a broker
who did nothing in May gets April compared against June and the report says they were flat
when they were absent. Cross joining the broker list against the month spine and left joining
the aggregate produces real zeros.

**They will ask: why the unique index on it?** `REFRESH MATERIALIZED VIEW CONCURRENTLY`
requires one. Without `CONCURRENTLY` the refresh takes an access-exclusive lock that blocks
every reader for its duration, which on a warehouse anyone queries during the load window is
the difference between a refresh and an outage.

**Honest limit.** I create it fresh each build rather than refreshing it incrementally, and I
have not dealt with a refresh that takes longer than the window allows.

---

## Questions I should expect and cannot fully answer

Rehearse saying these calmly. A confident "not yet, and here is how I would approach it" is
a much better answer than a vague one.

- **"How would this run on a schedule?"** I have not orchestrated it. There is no Airflow or
  dbt here, no retry policy, no alerting. The pieces that an orchestrator needs are in place,
  a run log, watermarks, non-zero exits and gates, but I have not wired one up.
- **"How do you handle a schema change from a source system?"** Today it breaks the `COPY`
  loudly, which I would argue is the right default. I have not built schema evolution.
- **"What about concurrency? Two loads at once?"** Nothing stops it. There is no advisory
  lock on the run, and `TRUNCATE` on the intermediate tables would be destructive if two runs
  overlapped. A single-run advisory lock is what I would add first.
- **"Have you tuned `work_mem` or `shared_buffers`?"** No. Both are container defaults, on
  purpose, because the small `work_mem` is what makes the disk spill in case 3 a real
  observation. I understand what they do; I have not sized them for a workload.
- **"Row-level security, roles, grants?"** None. Everything runs as the owner. I know
  `mart` should be readable and `raw` should not be, and I have not built that.
- **"Backups, point-in-time recovery, replication?"** Not touched. I know the words. That is
  a database administration skill I do not have yet.
- **"How big is too big for this design?"** My honest answer is that I do not know from
  experience, and then the reasoned answer: the parts I would expect to break first are the
  full rebuild of the intermediate layer and the sort-heavy transforms, because both scale
  linearly with total volume rather than with the batch. The README has a section on what I
  would change at 100x, and every item in it names the specific thing that breaks.
- **"Did you write all of this?"** Yes, with an AI assistant, the same way I work every day.
  What I can prove is that I can explain any line of it, defend the design decisions, and
  name the trade-offs I chose against, which is this file.
