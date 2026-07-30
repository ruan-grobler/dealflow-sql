#!/usr/bin/env python3
"""Build results.html from the LIVE output of sql/05_analytics.sql.

Nothing in the generated page is transcribed. Every table below is the actual
result set psql returned for the query printed above it, captured in the same
process run, so the page cannot drift away from the SQL the way a hand pasted
one does. If a query stops running, this script fails instead of shipping a
stale number.

Usage:
    python3 build_results.py            # writes results.html
    python3 build_results.py --out x    # writes somewhere else
"""

from __future__ import annotations

import argparse
import html
import os
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parent
ANALYTICS_SQL = REPO / "sql" / "05_analytics.sql"
ENV_FILE = REPO / ".env"

CONTAINER = os.environ.get("DEALFLOW_CONTAINER", "dealflow-db")
DB_USER = os.environ.get("POSTGRES_USER", "dealflow")
DB_NAME = os.environ.get("POSTGRES_DB", "dealflow")

# The unit separator. Any printable delimiter can appear inside a text column
# (property names carry spaces, notes carry commas), and a column that eats the
# delimiter shifts every column after it.
SEP = "\x1f"


# ---------------------------------------------------------------------------
# 1. Parsing the canonical SQL file
# ---------------------------------------------------------------------------

# One block is: a rule, a comment header, a rule, then SQL up to the next rule.
# The header pattern must EXCLUDE rule lines, because a rule also starts with
# "--". Without the negative lookahead the header group swallows every comment
# line in the file, including the rules, and the whole file parses as one query.
BLOCK_RE = re.compile(
    r"^-- ={10,}\n((?:^--(?!\s*={10,})[^\n]*\n)+)^-- ={10,}\n(.*?)(?=^-- ={10,}\n|\Z)",
    re.MULTILINE | re.DOTALL,
)
TITLE_RE = re.compile(r"^--\s*(Q\d+)\.\s*(.+?)\s*$", re.MULTILINE)
FIELD_RE = re.compile(
    r"^--\s*(BUSINESS QUESTION|TECHNIQUE):\s*(.*?)(?=^--\s*[A-Z][A-Z ]+:|\Z)",
    re.MULTILINE | re.DOTALL,
)


@dataclass
class Query:
    qid: str
    title: str
    question: str = ""
    technique: str = ""
    sql: str = ""
    headers: list[str] = field(default_factory=list)
    rows: list[list[str]] = field(default_factory=list)
    truncated: int = 0


def unwrap(comment_text: str) -> str:
    """Turn a wrapped SQL comment into one paragraph of prose."""
    lines = [re.sub(r"^--\s?", "", ln).strip() for ln in comment_text.splitlines()]
    return re.sub(r"\s+", " ", " ".join(ln for ln in lines if ln)).strip()


def parse_queries(path: Path) -> list[Query]:
    text = path.read_text()
    out: list[Query] = []
    for header, body in BLOCK_RE.findall(text):
        m = TITLE_RE.search(header)
        if not m:
            continue  # the file's own preamble, not a query
        q = Query(qid=m.group(1), title=m.group(2))
        for name, value in FIELD_RE.findall(header):
            prose = unwrap(value)
            if name == "BUSINESS QUESTION":
                q.question = prose
            else:
                q.technique = prose
        # psql meta commands are for interactive use and are not part of the
        # query, so they never reach the page or the server.
        sql = "\n".join(
            ln for ln in body.strip().splitlines() if not ln.startswith("\\")
        ).strip()
        if not sql:
            continue
        q.sql = sql
        out.append(q)
    return out


# ---------------------------------------------------------------------------
# 2. Running it
# ---------------------------------------------------------------------------

def load_password() -> str:
    if "POSTGRES_PASSWORD" in os.environ:
        return os.environ["POSTGRES_PASSWORD"]
    if not ENV_FILE.exists():
        sys.exit(f"cannot find {ENV_FILE}. Run ./run.sh once to create it.")
    for line in ENV_FILE.read_text().splitlines():
        if line.strip().startswith("POSTGRES_PASSWORD="):
            return line.split("=", 1)[1].strip().strip("'\"")
    sys.exit("POSTGRES_PASSWORD is not set in .env")


class Psql:
    """Runs one statement in the container and returns rows. The password is
    passed through the environment, never on the command line, because argv is
    world readable through ps(1)."""

    def __init__(self, password: str) -> None:
        self.env = {**os.environ, "PGPASSWORD": password}

    def query(self, sql: str) -> tuple[list[str], list[list[str]]]:
        proc = subprocess.run(
            ["docker", "exec", "-i", "-e", "PGPASSWORD", CONTAINER,
             "psql", "-U", DB_USER, "-d", DB_NAME, "-v", "ON_ERROR_STOP=1",
             "-A", "-F", SEP, "--pset", "footer=off", "-c", sql],
            input=b"", env=self.env, capture_output=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode().strip()[-2000:])
        lines = proc.stdout.decode().splitlines()
        if not lines:
            return [], []
        return lines[0].split(SEP), [ln.split(SEP) for ln in lines[1:]]

    def scalar(self, sql: str) -> str:
        _, rows = self.query(sql)
        return rows[0][0] if rows else ""


# ---------------------------------------------------------------------------
# 3. Rendering
# ---------------------------------------------------------------------------

KEYWORDS = {
    "select", "from", "where", "group", "by", "order", "having", "join", "left",
    "right", "inner", "outer", "on", "as", "with", "and", "or", "not", "in",
    "case", "when", "then", "else", "end", "over", "partition", "filter",
    "distinct", "union", "all", "cross", "lateral", "asc", "desc", "limit",
    "between", "is", "null", "using", "window", "recursive", "interval",
    "date", "exists", "true", "false", "nulls", "first", "last", "within",
    "cast", "rows", "range", "preceding", "following", "unbounded", "current",
    "row",
}
FUNCTIONS = {
    "count", "sum", "avg", "min", "max", "round", "coalesce", "rank",
    "dense_rank", "row_number", "lag", "lead", "percentile_cont", "greatest",
    "least", "extract", "date_trunc", "to_char", "generate_series", "nullif",
    "abs", "array_agg", "stddev_samp", "corr", "width_bucket", "first_value",
    "ntile", "cume_dist", "concat_ws", "make_date", "sqrt", "ln", "exp",
}
TOKEN_RE = re.compile(r"(--[^\n]*|'[^']*'|\b[A-Za-z_][A-Za-z0-9_]*\b|\s+|.)")


def highlight(sql: str) -> str:
    parts: list[str] = []
    for tok in TOKEN_RE.findall(sql):
        if tok.startswith("--"):
            parts.append(f'<span class="c">{html.escape(tok)}</span>')
        elif tok.startswith("'"):
            parts.append(f'<span class="s">{html.escape(tok)}</span>')
        elif tok.lower() in KEYWORDS and tok.isalpha():
            parts.append(f'<span class="k">{html.escape(tok)}</span>')
        elif tok.lower() in FUNCTIONS:
            parts.append(f'<span class="f">{html.escape(tok)}</span>')
        else:
            parts.append(html.escape(tok))
    return "".join(parts)


NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")


def cell(value: str) -> str:
    if value == "":
        return '<td class="null">null</td>'
    if NUMERIC_RE.match(value):
        # Thousands separators on integers only. Grouping a percentage or a
        # ratio would be noise, and grouping a year would be wrong.
        if "." not in value and abs(int(value)) >= 10000:
            return f'<td class="n">{int(value):,}</td>'
        return f'<td class="n">{html.escape(value)}</td>'
    return f"<td>{html.escape(value)}</td>"


def table(headers: list[str], rows: list[list[str]]) -> str:
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = "\n".join("<tr>" + "".join(cell(c) for c in r) + "</tr>" for r in rows)
    return f'<div class="scroll"><table><thead><tr>{head}</tr></thead><tbody>\n{body}\n</tbody></table></div>'


CSS = """
:root{--bg:#fbfbfa;--fg:#1b1b19;--dim:#6b6b66;--line:#e3e3de;--card:#fff;
--accent:#7a3e12;--kw:#8a2f7b;--fn:#0f6b5c;--str:#9a5a00;--cmt:#8c8c86;--num:#123f6b}
@media (prefers-color-scheme:dark){:root{--bg:#16161a;--fg:#e9e9e4;--dim:#9a9a93;
--line:#2c2c33;--card:#1d1d22;--accent:#e0a273;--kw:#d9a2d2;--fn:#7fd8c4;
--str:#e0be80;--cmt:#7d7d77;--num:#9ec5ea}}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--fg);
font:15px/1.6 ui-sans-serif,system-ui,-apple-system,"Segoe UI",Helvetica,Arial,sans-serif}
.wrap{max-width:1180px;margin:0 auto;padding:40px 22px 90px}
header{border-bottom:2px solid var(--fg);padding-bottom:22px;margin-bottom:12px}
h1{font-size:31px;margin:0 0 6px;letter-spacing:-.02em}
.sub{color:var(--dim);font-size:15px;margin:0}
.synth{margin:20px 0 0;padding:11px 14px;border-left:3px solid var(--accent);
background:var(--card);font-size:14px}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
gap:11px;margin:26px 0 34px}
.stat{background:var(--card);border:1px solid var(--line);border-radius:7px;padding:13px 15px}
.stat .v{font-size:21px;font-weight:650;font-variant-numeric:tabular-nums}
.stat .l{color:var(--dim);font-size:12px;text-transform:uppercase;letter-spacing:.05em;margin-top:3px}
.q{border-top:1px solid var(--line);padding-top:30px;margin-top:34px}
.qid{color:var(--accent);font:600 12px/1 ui-monospace,SFMono-Regular,Menlo,monospace;
letter-spacing:.09em}
h2{font-size:21px;margin:8px 0 12px;letter-spacing:-.01em}
.meta{margin:0 0 8px;font-size:14px}
.meta b{color:var(--dim);font-weight:600;text-transform:uppercase;font-size:11.5px;
letter-spacing:.05em;display:block;margin-bottom:2px}
.meta p{margin:0 0 12px;color:var(--fg)}
pre{background:var(--card);border:1px solid var(--line);border-radius:7px;
padding:14px 16px;overflow-x:auto;margin:14px 0;
font:13px/1.55 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
pre .k{color:var(--kw);font-weight:600}
pre .f{color:var(--fn)}
pre .s{color:var(--str)}
pre .c{color:var(--cmt);font-style:italic}
.scroll{overflow-x:auto;border:1px solid var(--line);border-radius:7px;background:var(--card)}
table{border-collapse:collapse;width:100%;font-size:13.5px}
th,td{padding:7px 11px;text-align:left;border-bottom:1px solid var(--line);white-space:nowrap}
th{background:color-mix(in srgb,var(--line) 45%,transparent);font-size:11.5px;
text-transform:uppercase;letter-spacing:.045em;color:var(--dim);position:sticky;top:0}
tbody tr:last-child td{border-bottom:none}
td.n{text-align:right;font-variant-numeric:tabular-nums;color:var(--num)}
td.null{color:var(--dim);font-style:italic}
.note{color:var(--dim);font-size:12.5px;margin:7px 0 0}
footer{margin-top:60px;padding-top:20px;border-top:1px solid var(--line);
color:var(--dim);font-size:13px}
code{font:12.5px ui-monospace,SFMono-Regular,Menlo,monospace;
background:color-mix(in srgb,var(--line) 45%,transparent);padding:1px 5px;border-radius:4px}
"""

MAX_ROWS = 30


def build_page(queries: list[Query], stats: list[tuple[str, str]], when: str) -> str:
    stat_html = "\n".join(
        f'<div class="stat"><div class="v">{html.escape(v)}</div>'
        f'<div class="l">{html.escape(l)}</div></div>'
        for l, v in stats
    )
    blocks = []
    for q in queries:
        note = ""
        if q.truncated:
            note = (f'<p class="note">Showing the first {MAX_ROWS} of '
                    f'{q.truncated:,} rows.</p>')
        blocks.append(f"""
<section class="q">
  <div class="qid">{html.escape(q.qid)}</div>
  <h2>{html.escape(q.title)}</h2>
  <div class="meta">
    <b>Business question</b><p>{html.escape(q.question)}</p>
    <b>Technique</b><p>{html.escape(q.technique)}</p>
  </div>
  <pre>{highlight(q.sql)}</pre>
  {table(q.headers, q.rows[:MAX_ROWS])}
  {note}
</section>""")

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>DealFlow SQL: query results</title>
<style>{CSS}</style>
</head><body><div class="wrap">
<header>
  <h1>DealFlow SQL</h1>
  <p class="sub">A commercial property deal pipeline warehouse in PostgreSQL 16.
  Fourteen analysis queries, and the real output of each one.</p>
</header>
<p class="synth"><b>All data on this page is synthetic</b>, generated by
<code>generate_data.py</code> from a fixed seed. No real broker, agency, property,
deal or client appears anywhere in it. Every table below was captured live from
the database by <code>build_results.py</code> and is not transcribed.</p>
<div class="stats">{stat_html}</div>
{"".join(blocks)}
<footer>
  Generated {html.escape(when)} from <code>sql/05_analytics.sql</code> against the
  loaded warehouse. Rebuild with <code>./run.sh results</code>.
</footer>
</div></body></html>
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", default=str(REPO / "results.html"))
    args = ap.parse_args()

    queries = parse_queries(ANALYTICS_SQL)
    if not queries:
        sys.exit(f"parsed no queries out of {ANALYTICS_SQL}")

    psql = Psql(load_password())

    print(f"running {len(queries)} queries from {ANALYTICS_SQL.name}")
    for q in queries:
        try:
            q.headers, q.rows = psql.query(q.sql)
        except RuntimeError as exc:
            # Never ship a page with a hole in it. A query that does not run is
            # a broken repository, not a missing section.
            sys.exit(f"{q.qid} failed, refusing to build a partial page:\n{exc}")
        if len(q.rows) > MAX_ROWS:
            q.truncated = len(q.rows)
        print(f"  {q.qid}  {len(q.rows):>6} rows  {q.title[:58]}")

    stats = [
        ("Stage event fact rows", f"{int(psql.scalar('SELECT count(*) FROM mart.fct_deal_stage_event')):,}"),
        ("Deals (one row each)", f"{int(psql.scalar('SELECT count(*) FROM mart.fct_deal_pipeline')):,}"),
        ("Fact partitions", psql.scalar(
            "SELECT count(*) FROM pg_inherits WHERE inhparent = 'mart.fct_deal_stage_event'::regclass")),
        ("SCD2 broker versions", f"{int(psql.scalar('SELECT count(*) FROM mart.dim_broker')):,}"),
        ("SCD2 property versions", f"{int(psql.scalar('SELECT count(*) FROM mart.dim_property')):,}"),
        ("Quality gates passing", psql.scalar("""
            SELECT count(*) FILTER (WHERE is_pass) || ' / ' || count(*)
            FROM dq.check_result
            WHERE run_id = (SELECT max(run_id) FROM dq.check_result)""")),
        ("Star schema size", psql.scalar("""
            SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid)))
            FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'mart' AND c.relkind IN ('r', 'p', 'm')""")),
    ]

    when = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    Path(args.out).write_text(build_page(queries, stats, when))
    print(f"\nwrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
