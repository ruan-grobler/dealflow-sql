#!/usr/bin/env python3
"""Re-runnable performance benchmark for the DealFlow warehouse.

Reads sql/06_performance.sql, which is the single source of truth for the fixture, the
indexes and the case study queries, and executes it in the right order:

    1. build or verify the fixture (idempotent, skips work already done)
    2. for every case: establish the BEFORE state, time the BEFORE query, capture its plan,
       establish the AFTER state, time the AFTER query, capture its plan
    3. capture the partition pruning and supporting evidence plans
    4. restore the accepted index state and read index usage out of pg_stat_user_indexes
    5. print the results table

Nothing here invents a number. Every figure printed is parsed out of PostgreSQL's own
output, and the captured plans are written to plans/ so a reader can check the claims.

MEASUREMENT METHOD
    Each query runs once to warm the cache, then --runs times more. Reported figures are the
    MEDIAN of the timed runs with min and max alongside, so a reader can see the spread
    instead of trusting a single sample. Timings come from psql's own \\timing, taken inside
    one already open session, so they are client observed round trip and therefore INCLUDE
    planning time. That is deliberate: planning time is part of what a user waits for, and in
    case study 1 it turns out to be most of the win.

    EXPLAIN (ANALYZE, BUFFERS) is captured separately and used only for plan shape and buffer
    counts, never for the headline timings. Its per node instrumentation is not free: on an
    85 partition plan it inflated one measured query from 3.9 ms to 36.2 ms.

WHY psql AND NOT psycopg
    No third party driver is needed, so the suite runs anywhere Docker runs. Every repetition
    of a query happens inside a single psql session, so process startup is paid once per case
    rather than once per timed run, which matters when the fastest query here is under 4 ms.

USAGE
    python3 benchmark.py                  # full run
    python3 benchmark.py --runs 9         # more repetitions
    python3 benchmark.py --rebuild        # drop the perf schema and rebuild from scratch
    python3 benchmark.py --case case3     # one case only
    python3 benchmark.py --skip-evidence  # timings only, no plan capture
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
SQL_FILE = REPO_ROOT / "sql" / "06_performance.sql"
PLANS_DIR = REPO_ROOT / "plans"
ENV_FILE = REPO_ROOT / ".env"

CONTAINER = os.environ.get("DEALFLOW_CONTAINER", "dealflow-db")
DB_USER = os.environ.get("POSTGRES_USER", "dealflow")
DB_NAME = os.environ.get("POSTGRES_DB", "dealflow")

TIMING_RE = re.compile(r"^Time:\s+([0-9.]+)\s+ms", re.MULTILINE)


# --------------------------------------------------------------------------------------
# Parsing sql/06_performance.sql
# --------------------------------------------------------------------------------------
@dataclass
class Case:
    """One before and after case study, assembled from the @block directives."""

    case_id: str
    title: str = ""
    question: str = ""
    diagnosis: str = ""
    fix: str = ""
    before_setup: str = ""
    before_query: str = ""
    after_setup: str = ""
    after_query: str = ""


@dataclass
class Script:
    """Everything parsed out of the SQL file."""

    preflight: list[tuple[str, str]] = field(default_factory=list)
    fixture: list[tuple[str, str]] = field(default_factory=list)
    evidence: list[tuple[str, str]] = field(default_factory=list)
    indexcheck: list[tuple[str, str]] = field(default_factory=list)
    cases: dict[str, Case] = field(default_factory=dict)
    case_order: list[str] = field(default_factory=list)


BLOCK_RE = re.compile(r"^--\s*@block\s+(\S+)\s+(\S+)\s*$")
CASE_RE = re.compile(r"^--\s*@case\s+(\S+)\s*$")
META_RE = re.compile(r"^--\s*@(title|question|diagnosis|fix)\s+(.*)$")
META_CONT_RE = re.compile(r"^--\s{6,}(\S.*)$")


def parse_sql(path: Path) -> Script:
    """Parse the @block and @case directives out of the canonical SQL file.

    Keeping the queries in the SQL file and parsing them here means there is exactly one copy
    of every query and every index in the repository. A harness that held its own copies would
    drift away from the file a reviewer actually reads.
    """
    script = Script()
    current_block: tuple[str, str] | None = None
    body: list[str] = []
    current_case: Case | None = None
    last_meta: str | None = None

    def flush() -> None:
        if current_block is None:
            return
        kind, name = current_block
        sql = "\n".join(body).strip()
        if not sql:
            return
        if kind == "preflight":
            script.preflight.append((name, sql))
        elif kind == "fixture":
            script.fixture.append((name, sql))
        elif kind == "evidence":
            script.evidence.append((name, sql))
        elif kind == "indexcheck":
            script.indexcheck.append((name, sql))
        elif kind in {"before-setup", "before-query", "after-setup", "after-query"}:
            case = script.cases[name]
            setattr(case, kind.replace("-", "_"), sql)
        else:
            raise ValueError(f"unknown block kind {kind!r} for {name!r}")

    for raw in path.read_text().splitlines():
        # Both directives terminate whatever block is open, so a block body runs from its own
        # @block line to the next directive of either kind and never swallows one.
        block_match = BLOCK_RE.match(raw)
        if block_match:
            flush()
            kind, name = block_match.group(1), block_match.group(2)
            if kind.endswith("-setup") or kind.endswith("-query"):
                if name not in script.cases:
                    raise ValueError(f"block for unknown case {name!r}, declare @case first")
            current_block = (kind, name)
            body = []
            last_meta = None
            continue

        case_match = CASE_RE.match(raw)
        if case_match:
            flush()
            current_block = None
            body = []
            case_id = case_match.group(1)
            current_case = Case(case_id=case_id)
            script.cases[case_id] = current_case
            script.case_order.append(case_id)
            last_meta = None
            continue

        if current_block is not None:
            body.append(raw)
            continue

        meta_match = META_RE.match(raw)
        if meta_match and current_case is not None:
            key, value = meta_match.group(1), meta_match.group(2).strip()
            setattr(current_case, key, value)
            last_meta = key
            continue

        cont_match = META_CONT_RE.match(raw)
        if cont_match and current_case is not None and last_meta:
            prev = getattr(current_case, last_meta)
            setattr(current_case, last_meta, f"{prev} {cont_match.group(1).strip()}")
            continue

        last_meta = None

    flush()
    return script


# --------------------------------------------------------------------------------------
# Talking to PostgreSQL
# --------------------------------------------------------------------------------------
def load_password() -> str:
    """Read the password from the environment or .env. It is never printed or logged."""
    if os.environ.get("POSTGRES_PASSWORD"):
        return os.environ["POSTGRES_PASSWORD"]
    if not ENV_FILE.exists():
        sys.exit(f"no POSTGRES_PASSWORD in the environment and no {ENV_FILE}")
    for line in ENV_FILE.read_text().splitlines():
        line = line.strip()
        if line.startswith("POSTGRES_PASSWORD="):
            return line.split("=", 1)[1].strip().strip("'\"")
    sys.exit(f"POSTGRES_PASSWORD not found in {ENV_FILE}")


class Psql:
    """Runs a SQL payload in the container and returns stdout."""

    def __init__(self, password: str) -> None:
        self._password = password
        if shutil.which("docker") is None:
            sys.exit("docker is not on PATH, cannot reach the database container")

    def run(self, sql: str, *, stop_on_error: bool = True) -> str:
        cmd = [
            "docker", "exec", "-i",
            "-e", f"PGPASSWORD={self._password}",
            CONTAINER, "psql", "-U", DB_USER, "-d", DB_NAME,
        ]
        if stop_on_error:
            cmd += ["-v", "ON_ERROR_STOP=1"]
        completed = subprocess.run(
            cmd, input=sql, capture_output=True, text=True, timeout=1800, check=False
        )
        out = completed.stdout + completed.stderr
        if stop_on_error and completed.returncode != 0:
            raise RuntimeError(f"psql failed:\n{out.strip()[-4000:]}")
        return out


# --------------------------------------------------------------------------------------
# Measurement
# --------------------------------------------------------------------------------------
@dataclass
class Timing:
    """The timed runs for one query, plus the derived statistics."""

    samples: list[float]
    warmup_ms: float
    rival_backends: int = 0

    @property
    def median_ms(self) -> float:
        return statistics.median(self.samples)

    @property
    def min_ms(self) -> float:
        return min(self.samples)

    @property
    def max_ms(self) -> float:
        return max(self.samples)

    @property
    def spread_ratio(self) -> float:
        """max / min. A benchmark on a quiet box sits close to 1."""
        return self.max_ms / self.min_ms if self.min_ms else float("inf")


# A sample above this ratio is not a measurement of the query, it is a measurement of
# whatever else was running. It gets flagged rather than quietly averaged in.
SPREAD_WARN_RATIO = 3.0


def count_rival_backends(psql: Psql) -> int:
    """How many other sessions are doing work in this database right now.

    A benchmark that shares its container with a concurrent bulk load produces numbers that
    look like measurements and are not. This was not hypothetical: an early run of this suite
    overlapped a warehouse load and reported a before figure of 12,173 ms for case 3 against
    2,830 ms on a quiet box, with a min to max spread of 28x on another case. So the harness
    now checks, prints what it finds, and marks any affected result.
    """
    out = psql.run(
        "\\pset tuples_only on\n"
        "\\pset format unaligned\n"
        "SELECT count(*) FROM pg_stat_activity\n"
        " WHERE datname = current_database()\n"
        "   AND pid <> pg_backend_pid()\n"
        "   AND state <> 'idle';\n"
    )
    for line in out.splitlines():
        if line.strip().isdigit():
            return int(line.strip())
    return 0


def time_query(psql: Psql, query: str, runs: int) -> Timing:
    """Run one query warmup + `runs` times in a single session and return every sample.

    Results are discarded to /dev/null on the server side of psql so that formatting and
    printing rows does not get counted as query time. Every repetition happens in the same
    session, so connection setup is not in any sample.
    """
    stripped = query.strip().rstrip(";")
    lines = [
        "\\set ON_ERROR_STOP on",
        "SET statement_timeout = '600s';",
        # The container default. Stated explicitly so a stray session setting cannot silently
        # change what is being measured.
        "SET work_mem = '4MB';",
        "\\timing on",
        f"{stripped} \\g /dev/null",  # warmup, reported separately, never in the median
    ]
    for _ in range(runs):
        lines.append(f"{stripped} \\g /dev/null")
    rivals_before = count_rival_backends(psql)
    out = psql.run("\n".join(lines) + "\n")
    rivals_after = count_rival_backends(psql)
    samples = [float(x) for x in TIMING_RE.findall(out)]
    if len(samples) < runs + 1:
        raise RuntimeError(
            f"expected {runs + 1} timing lines, parsed {len(samples)}:\n{out.strip()[-2000:]}"
        )
    # psql emits one Time: line per statement; the SET statements come before \timing on, so
    # the first parsed sample is the warmup and the rest are the timed runs.
    return Timing(samples=samples[1: runs + 1], warmup_ms=samples[0],
                  rival_backends=max(rivals_before, rivals_after))


def capture_plan(psql: Psql, query: str) -> str:
    """Capture EXPLAIN (ANALYZE, BUFFERS) for one query, warming the cache first."""
    stripped = query.strip().rstrip(";")
    payload = "\n".join([
        "\\set ON_ERROR_STOP on",
        "SET statement_timeout = '600s';",
        "SET work_mem = '4MB';",
        f"{stripped} \\g /dev/null",
        "\\pset pager off",
        f"EXPLAIN (ANALYZE, BUFFERS) {stripped};",
    ]) + "\n"
    return psql.run(payload)


PLAN_SUMMARY_KEYS = (
    "Planning Time:",
    "Execution Time:",
    "Subplans Removed:",
    "Sort Method:",
    "Heap Fetches:",
    "Workers Launched:",
)


def plan_facts(plan: str) -> dict[str, str]:
    """Pull the handful of plan facts worth quoting in a summary table."""
    facts: dict[str, str] = {}
    for line in plan.splitlines():
        text = line.strip()
        for key in PLAN_SUMMARY_KEYS:
            if text.startswith(key):
                facts.setdefault(key.rstrip(":"), text[len(key):].strip())
    # Count only heap access nodes. "Bitmap Index Scan on fct_deal_stage_event_2026_02_event_ts_idx"
    # names an index, not a partition, and counting it would double report every partition.
    partitions = len(re.findall(r"(?:Seq Scan|Parallel Seq Scan|Index Scan|Index Only Scan|"
                               r"Bitmap Heap Scan|Parallel Bitmap Heap Scan) on "
                               r"fct_deal_stage_event_(?!\S*_idx\b)\S+", plan))
    if partitions:
        facts["Fact partitions in plan"] = str(partitions)
    hit = sum(int(m) for m in re.findall(r"shared hit=(\d+)", plan))
    read = sum(int(m) for m in re.findall(r"shared read=(\d+)", plan))
    if hit or read:
        facts["Top level buffers"] = _top_level_buffers(plan)
    return facts


def _top_level_buffers(plan: str) -> str:
    """The first Buffers line in the plan is the whole query's total."""
    for line in plan.splitlines():
        text = line.strip()
        if text.startswith("Buffers: shared"):
            return text[len("Buffers: "):]
    return "n/a"


# --------------------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------------------
@dataclass
class Result:
    case_id: str
    title: str
    before: Timing
    after: Timing

    @property
    def speedup(self) -> float:
        return self.before.median_ms / self.after.median_ms


def fmt_ms(value: float) -> str:
    return f"{value:,.1f}"


def print_table(results: list[Result], runs: int) -> None:
    header = ("case", "query", "before ms", "after ms", "speedup")
    rows: list[tuple[str, str, str, str, str]] = []
    for r in results:
        speed = r.speedup
        verdict = f"{speed:,.1f}x" if speed >= 1.05 else (
            f"{speed:,.2f}x (no gain)" if speed >= 0.95 else f"{speed:,.2f}x (SLOWER)"
        )
        rows.append((r.case_id, r.title[:52], fmt_ms(r.before.median_ms),
                     fmt_ms(r.after.median_ms), verdict))

    widths = [max(len(header[i]), *(len(row[i]) for row in rows)) for i in range(5)]
    line = "  ".join("-" * w for w in widths)
    print()
    print(f"MEASURED RESULTS. Median of {runs} timed runs after one warmup run.")
    print("Timings are client observed round trip and include planning time.")
    print()
    print("  ".join(h.ljust(widths[i]) for i, h in enumerate(header)))
    print(line)
    for row in rows:
        print("  ".join(row[i].ljust(widths[i]) for i in range(5)))
    print(line)
    print()
    print("SPREAD (min / median / max, milliseconds). A quiet box keeps max/min near 1.")
    suspect = False
    for r in results:
        for label, t in (("before", r.before), ("after", r.after)):
            flag = ""
            if t.rival_backends:
                flag = f"  <-- {t.rival_backends} other active backend(s) during this run"
                suspect = True
            elif t.spread_ratio > SPREAD_WARN_RATIO:
                flag = f"  <-- max/min {t.spread_ratio:,.1f}x, treat with suspicion"
                suspect = True
            print(f"  {r.case_id if label == 'before' else '':<8} {label:<7}"
                  f" {fmt_ms(t.min_ms):>10} / {fmt_ms(t.median_ms):>10} /"
                  f" {fmt_ms(t.max_ms):>10}{flag}")
    if suspect:
        print()
        print("  WARNING: at least one result above was measured on a busy or unstable system.")
        print("  Re-run on an idle container before quoting those numbers anywhere.")
    else:
        print()
        print("  No competing backends were seen and every spread is within tolerance.")


def print_env(psql: Psql) -> None:
    out = psql.run(
        "\\pset tuples_only on\n"
        "\\pset format unaligned\n"
        "\\pset fieldsep ' | '\n"
        "SELECT version();\n"
        "SELECT 'shared_buffers=' || current_setting('shared_buffers')"
        " || ' work_mem=' || current_setting('work_mem')"
        " || ' max_parallel_workers_per_gather=' || current_setting('max_parallel_workers_per_gather')"
        " || ' TimeZone=' || current_setting('TimeZone');\n"
    )
    print("ENVIRONMENT UNDER TEST")
    for line in out.splitlines():
        if line.strip():
            print(f"  {line.strip()}")
    print()


def print_fixture_shape(psql: Psql) -> None:
    out = psql.run(
        "\\pset tuples_only on\n"
        "\\pset format unaligned\n"
        "\\pset fieldsep ' | '\n"
        "SELECT 'fct_deal_stage_event rows', count(*)::text FROM perf.fct_deal_stage_event\n"
        "UNION ALL SELECT 'fct_deal_stage_event partitions', count(*)::text FROM pg_inherits"
        " WHERE inhparent = 'perf.fct_deal_stage_event'::regclass\n"
        "UNION ALL SELECT 'fct_deal_pipeline rows', count(*)::text FROM perf.fct_deal_pipeline\n"
        "UNION ALL SELECT 'raw_stage_event rows', count(*)::text FROM perf.raw_stage_event\n"
        "UNION ALL SELECT 'dim_broker rows (SCD2)', count(*)::text FROM perf.dim_broker\n"
        "UNION ALL SELECT 'fact heap size',"
        " pg_size_pretty(sum(pg_relation_size(c.oid))) FROM pg_class c"
        " JOIN pg_inherits i ON i.inhrelid = c.oid"
        " WHERE i.inhparent = 'perf.fct_deal_stage_event'::regclass;\n"
    )
    print("FIXTURE UNDER TEST")
    for line in out.splitlines():
        if line.strip():
            print(f"  {line.strip()}")
    print()


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------
def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--runs", type=int, default=5,
                        help="timed runs per query after one warmup run (default 5)")
    parser.add_argument("--rebuild", action="store_true",
                        help="drop the perf schema and rebuild the fixture from scratch")
    parser.add_argument("--case", action="append", default=None,
                        help="run only this case id, repeatable")
    parser.add_argument("--skip-evidence", action="store_true",
                        help="skip plan and evidence capture, timings only")
    args = parser.parse_args()

    if args.runs < 3:
        sys.exit("--runs must be at least 3 for a median to mean anything")

    script = parse_sql(SQL_FILE)
    psql = Psql(load_password())
    PLANS_DIR.mkdir(exist_ok=True)

    started = time.monotonic()
    print("=" * 78)
    print("DEALFLOW WAREHOUSE PERFORMANCE BENCHMARK")
    print("=" * 78)
    print()
    print_env(psql)

    if args.rebuild:
        print("rebuilding the fixture from scratch (DROP SCHEMA perf CASCADE)")
        psql.run("DROP SCHEMA IF EXISTS perf CASCADE;")

    print("PREFLIGHT AND FIXTURE")
    for name, sql in script.preflight + script.fixture:
        t0 = time.monotonic()
        out = psql.run(sql)
        elapsed = (time.monotonic() - t0) * 1000
        notices = [ln.strip() for ln in out.splitlines() if ln.startswith("NOTICE:")]
        note = f"  {notices[-1][8:].strip()}" if notices else ""
        print(f"  {name:<28} {elapsed:>9,.1f} ms{note}")
    print()
    print_fixture_shape(psql)

    wanted = args.case or script.case_order
    results: list[Result] = []

    for case_id in wanted:
        case = script.cases.get(case_id)
        if case is None:
            sys.exit(f"unknown case {case_id!r}, known cases: {', '.join(script.case_order)}")
        if not case.before_query or not case.after_query:
            sys.exit(f"case {case_id!r} is missing a before or after query")

        print(f"CASE {case_id}: {case.title}")

        if case.before_setup:
            psql.run(case.before_setup)
        before = time_query(psql, case.before_query, args.runs)
        print(f"  before  median {fmt_ms(before.median_ms):>10} ms"
              f"  (warmup {fmt_ms(before.warmup_ms)} ms)")
        if not args.skip_evidence:
            plan = capture_plan(psql, case.before_query)
            (PLANS_DIR / f"{case_id}_before.txt").write_text(plan)
            for key, value in plan_facts(plan).items():
                print(f"      {key}: {value}")

        if case.after_setup:
            t0 = time.monotonic()
            psql.run(case.after_setup)
            print(f"  fix applied in {(time.monotonic() - t0) * 1000:,.1f} ms")
        after = time_query(psql, case.after_query, args.runs)
        print(f"  after   median {fmt_ms(after.median_ms):>10} ms"
              f"  (warmup {fmt_ms(after.warmup_ms)} ms)")
        if not args.skip_evidence:
            plan = capture_plan(psql, case.after_query)
            (PLANS_DIR / f"{case_id}_after.txt").write_text(plan)
            for key, value in plan_facts(plan).items():
                print(f"      {key}: {value}")

        results.append(Result(case_id=case_id, title=case.title, before=before, after=after))
        print()

    if not args.skip_evidence:
        print("EVIDENCE CAPTURE")
        for name, sql in script.evidence:
            out = psql.run(sql)
            (PLANS_DIR / f"evidence_{name}.txt").write_text(out)
            print(f"  plans/evidence_{name}.txt")
        print()

    print("INDEX JUSTIFICATION")
    for _, sql in script.indexcheck:
        out = psql.run(sql)
        if "idx_scan" in out or "index_name" in out:
            (PLANS_DIR / "index_usage.txt").write_text(out)
            print(out.strip())
    print()

    print_table(results, args.runs)

    print()
    print(f"total wall clock {time.monotonic() - started:,.1f} s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
