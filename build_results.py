#!/usr/bin/env python3
"""Build results.html from queries.sql and the live database.

Every table in the generated page is the actual output of running that query
against the loaded container. Nothing here is typed by hand.

Run it through ./run.sh results (which guarantees the database is loaded first).
"""

from __future__ import annotations

import csv
import html
import io
import os
import pathlib
import re
import subprocess
import sys
from datetime import date

ROOT = pathlib.Path(__file__).resolve().parent
CONTAINER = "dealflow-db"

SQL_KEYWORDS = {
    "select", "from", "where", "and", "or", "not", "in", "is", "null", "as",
    "join", "inner", "left", "right", "outer", "on", "group", "by", "having",
    "order", "asc", "desc", "limit", "offset", "exists", "case", "when", "then",
    "else", "end", "over", "partition", "distinct", "union", "all", "true",
    "false", "with", "between", "like",
}
SQL_FUNCS = {
    "count", "sum", "avg", "min", "max", "round", "coalesce", "nullif",
    "date_trunc", "rank", "dense_rank", "row_number", "cast",
}

TOKEN_RE = re.compile(
    r"(?P<comment>--[^\n]*)"
    r"|(?P<string>'(?:[^']|'')*')"
    r"|(?P<number>\b\d+(?:\.\d+)?\b)"
    r"|(?P<word>[A-Za-z_][A-Za-z_0-9]*)"
)


def env() -> dict[str, str]:
    values = {}
    env_file = ROOT / ".env"
    if not env_file.exists():
        sys.exit("No .env found. Run ./run.sh first.")
    for line in env_file.read_text().splitlines():
        if "=" in line and not line.strip().startswith("#"):
            key, _, val = line.partition("=")
            values[key.strip()] = val.strip()
    return values


def run_sql(sql: str, cfg: dict[str, str]) -> tuple[list[str], list[list[str]]]:
    """Run one statement and return (headers, rows) from real psql CSV output."""
    proc = subprocess.run(
        [
            "docker", "exec", "-i", CONTAINER,
            "psql", "-U", cfg["POSTGRES_USER"], "-d", cfg["POSTGRES_DB"],
            "-v", "ON_ERROR_STOP=1", "-q", "--csv",
        ],
        input=sql,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        sys.exit(f"Query failed:\n{sql}\n{proc.stderr}")
    reader = list(csv.reader(io.StringIO(proc.stdout)))
    if not reader:
        return [], []
    return reader[0], reader[1:]


def parse_queries(path: pathlib.Path) -> list[dict]:
    """Split queries.sql into blocks of (number, title, purpose lines, sql)."""
    lines = path.read_text().splitlines()
    starts = [i for i, line in enumerate(lines) if re.match(r"^-- Q\d+\.", line)]
    blocks = []
    for idx, start in enumerate(starts):
        stop = starts[idx + 1] if idx + 1 < len(starts) else len(lines)
        chunk = lines[start:stop]
        head = re.match(r"^-- Q(\d+)\.\s*(.*)$", chunk[0])
        purpose, sql_lines, in_sql = [], [], False
        for line in chunk[1:]:
            if not in_sql and line.startswith("--"):
                purpose.append(line[2:].strip())
            elif line.strip():
                in_sql = True
                sql_lines.append(line)
            elif in_sql:
                sql_lines.append(line)
        blocks.append({
            "n": head.group(1),
            "title": head.group(2).strip(),
            "purpose": " ".join(purpose).strip(),
            "sql": "\n".join(sql_lines).strip(),
        })
    return blocks


def highlight(sql: str) -> str:
    """Tiny SQL highlighter. No libraries, just spans the page's CSS styles."""
    out, pos = [], 0
    for match in TOKEN_RE.finditer(sql):
        out.append(html.escape(sql[pos:match.start()]))
        text = html.escape(match.group(0))
        kind = match.lastgroup
        if kind == "comment":
            out.append(f'<span class="c">{text}</span>')
        elif kind == "string":
            out.append(f'<span class="s">{text}</span>')
        elif kind == "number":
            out.append(f'<span class="n">{text}</span>')
        else:
            low = match.group(0).lower()
            if low in SQL_KEYWORDS:
                out.append(f'<span class="k">{text}</span>')
            elif low in SQL_FUNCS:
                out.append(f'<span class="f">{text}</span>')
            else:
                out.append(text)
        pos = match.end()
    out.append(html.escape(sql[pos:]))
    return "".join(out)


NUMERIC_RE = re.compile(r"^-?\d+(\.\d+)?$")


def fmt_cell(col: str, raw: str) -> tuple[str, bool]:
    """Return (display text, is_numeric) for one cell."""
    if raw == "":
        return '<span class="muted">none</span>', False
    if col in ("is_active", "needs_review"):
        if raw in ("t", "true"):
            return "Yes", False
        if raw in ("f", "false"):
            return "No", False
    if NUMERIC_RE.match(raw):
        value = float(raw)
        if col.endswith("_zar"):
            return "R" + f"{value:,.0f}", True
        if "." in raw and float(raw) != int(value):
            return f"{value:,.1f}", True
        return f"{int(value):,}", True
    return html.escape(raw), False


def render_table(headers: list[str], rows: list[list[str]]) -> str:
    if not headers:
        return '<p class="muted">No output.</p>'
    head = "".join(f"<th>{html.escape(h)}</th>" for h in headers)
    body = []
    for row in rows:
        cells = []
        for col, raw in zip(headers, row):
            text, numeric = fmt_cell(col, raw)
            cells.append(f'<td class="{"num" if numeric else ""}">{text}</td>')
        body.append("<tr>" + "".join(cells) + "</tr>")
    count = f"{len(rows)} row" + ("" if len(rows) == 1 else "s")
    return (
        '<div class="tw"><table><thead><tr>' + head + "</tr></thead><tbody>"
        + "".join(body) + "</tbody></table></div>"
        + f'<p class="rows">{count}</p>'
    )


CSS = """
:root{color-scheme:dark}
*{box-sizing:border-box}
body{margin:0;background:#0d1117;color:#d7dee8;
 font:16px/1.65 -apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif}
.wrap{max-width:940px;margin:0 auto;padding:48px 20px 90px}
h1{font-family:Georgia,"Times New Roman",serif;font-weight:600;font-size:34px;
 line-height:1.2;margin:0 0 10px;color:#f2f6fb;letter-spacing:-.01em}
.eyebrow{text-transform:uppercase;letter-spacing:.14em;font-size:11px;
 color:#7d8da3;margin:0 0 14px}
.lede{font-size:17px;color:#a9b6c7;margin:0 0 26px;max-width:70ch}
.meta{display:flex;flex-wrap:wrap;gap:8px;margin:0 0 34px;padding:0;list-style:none}
.meta li{font-size:12px;color:#9fb0c4;background:#161c26;border:1px solid #263042;
 border-radius:999px;padding:5px 11px}
.card{background:#111721;border:1px solid #222c3b;border-radius:12px;
 padding:22px 22px 16px;margin:0 0 22px}
h2{font-size:13px;text-transform:uppercase;letter-spacing:.1em;color:#6ea8fe;
 margin:0 0 6px;font-weight:700}
h2 .t{color:#e6edf6;text-transform:none;letter-spacing:0;font-weight:600;font-size:16px}
.purpose{margin:0 0 16px;color:#a9b6c7;font-size:15px;max-width:74ch}
pre{margin:0 0 18px;padding:16px;background:#0a0e14;border:1px solid #1d2532;
 border-radius:9px;overflow-x:auto;
 font:13.5px/1.6 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;color:#c9d4e2}
pre .k{color:#7aa2f7;font-weight:600}
pre .f{color:#9ece6a}
pre .s{color:#e0af68}
pre .n{color:#e0af68}
pre .c{color:#5d6b80;font-style:italic}
.tw{overflow-x:auto;border:1px solid #1d2532;border-radius:9px;background:#0a0e14}
table{border-collapse:collapse;width:100%;font-size:14px}
th,td{padding:9px 14px;text-align:left;white-space:nowrap;
 border-bottom:1px solid #1a2230}
th{background:#131a24;color:#8fa3ba;font-weight:600;font-size:12px;
 text-transform:uppercase;letter-spacing:.06em}
tbody tr:last-child td{border-bottom:none}
tbody tr:nth-child(even) td{background:#0c1119}
td.num{text-align:right;font-variant-numeric:tabular-nums;
 font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
.rows{margin:8px 0 0;font-size:12px;color:#6f8098}
.muted{color:#66748a}
.note{border-left:3px solid #2d4a7c;background:#101722;padding:14px 18px;
 border-radius:0 8px 8px 0;margin:0 0 30px;font-size:14.5px;color:#a9b6c7}
footer{margin-top:44px;padding-top:22px;border-top:1px solid #1d2532;
 font-size:13.5px;color:#7d8da3}
code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.92em;
 background:#0a0e14;border:1px solid #1d2532;border-radius:5px;padding:1px 6px;
 color:#c9d4e2}
@media (max-width:600px){.wrap{padding:32px 14px 60px}h1{font-size:27px}}
"""


def build(blocks: list[dict], cfg: dict[str, str]) -> str:
    counts = {}
    for name in ("brokers", "properties", "deals"):
        _, rows = run_sql(f"SELECT count(*) FROM {name};", cfg)
        counts[name] = rows[0][0]

    sections = []
    for block in blocks:
        headers, rows = run_sql(block["sql"] + "\n", cfg)
        sections.append(
            '<section class="card">'
            f'<h2>Q{block["n"]} &nbsp;<span class="t">{html.escape(block["title"])}</span></h2>'
            f'<p class="purpose">{html.escape(block["purpose"])}</p>'
            f"<pre>{highlight(block['sql'])}</pre>"
            + render_table(headers, rows)
            + "</section>"
        )

    stamp = date.today().isoformat()
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="robots" content="noindex">
<title>DealFlow SQL. 10 queries and their real output</title>
<style>{CSS}</style>
</head>
<body>
<div class="wrap">
<p class="eyebrow">SQL proof of work</p>
<h1>DealFlow SQL</h1>
<p class="lede">A small PostgreSQL database for a commercial property deal pipeline,
built to show working SQL rather than describe it. Ten queries, from a plain
SELECT to window functions. Every table on this page is the real output of the
query printed directly above it, captured from a live run.</p>
<ul class="meta">
<li>PostgreSQL 16 in Docker</li>
<li>{counts['brokers']} brokers</li>
<li>{counts['properties']} properties</li>
<li>{counts['deals']} deals</li>
<li>3 tables, 2 foreign keys, 3 indexes</li>
<li>Generated {stamp}</li>
</ul>
<p class="note"><strong>The data is invented.</strong> Brokers, agencies, properties
and values are synthetic seed data written for this project. No real company,
person, client or transaction appears anywhere in it. Reproduce the whole thing
with one command: <code>./run.sh</code></p>
{"".join(sections)}
<footer>
<p><strong>How it is put together.</strong> <code>schema.sql</code> creates three
tables (brokers and properties as lookups, deals as the fact table) with foreign
keys, a unique message id as the dedupe key, a check constraint on the five
allowed statuses, and three indexes. <code>load.sql</code> copies each CSV into a
temporary staging table of text columns, then inserts into the real tables,
resolving foreign keys by joining on broker email and property name, all inside
one transaction. <code>run.sh</code> starts the container, waits for readiness,
loads, and runs every query. Running it twice gives the same result.</p>
<p>Open pipeline throughout means status New, In review or Qualified.</p>
</footer>
</div>
</body>
</html>
"""


PORTFOLIO_PROOF = pathlib.Path(
    "/Users/admin/Code/impact-portfolio/proof/dealflow-sql.html"
)

BACK_LINK = (
    '<p style="margin:0 0 26px"><a href="../index.html" '
    'style="color:#6ea8fe;text-decoration:none;font-size:14px">'
    "&#8592;&nbsp; Back to the portfolio</a></p>"
)


def publish_to_portfolio(page: str) -> None:
    """Write the same page into the portfolio, with a back link added.

    Generated from the identical live run, so the portfolio copy can never
    drift away from what the database actually returns.
    """
    if not PORTFOLIO_PROOF.parent.parent.is_dir():
        return
    PORTFOLIO_PROOF.parent.mkdir(parents=True, exist_ok=True)
    marker = '<p class="eyebrow">'
    page = page.replace(marker, BACK_LINK + "\n" + marker, 1)
    PORTFOLIO_PROOF.write_text(page)
    print(f"Wrote {PORTFOLIO_PROOF}")


def rewrite_readme(blocks: list[dict], cfg: dict[str, str]) -> None:
    """Refresh the aligned psql output blocks in README.md so they stay true."""
    readme = ROOT / "README.md"
    if not readme.exists():
        return
    text = readme.read_text()
    for block in blocks:
        proc = subprocess.run(
            [
                "docker", "exec", "-i", CONTAINER,
                "psql", "-U", cfg["POSTGRES_USER"], "-d", cfg["POSTGRES_DB"],
                "-v", "ON_ERROR_STOP=1", "-q",
            ],
            input=block["sql"] + "\n",
            capture_output=True,
            text=True,
        )
        if proc.returncode != 0:
            continue
        pattern = re.compile(
            r"(### Q" + block["n"] + r"\..*?\n```\n).*?(\n```)",
            re.DOTALL,
        )
        if pattern.search(text):
            text = pattern.sub(
                lambda m: m.group(1) + proc.stdout.rstrip("\n") + m.group(2),
                text,
                count=1,
            )
    readme.write_text(text)


def main() -> None:
    cfg = env()
    blocks = parse_queries(ROOT / "queries.sql")
    if len(blocks) != 10:
        sys.exit(f"Expected 10 query blocks, parsed {len(blocks)}.")
    page = build(blocks, cfg)
    (ROOT / "results.html").write_text(page)
    print(f"Wrote {ROOT / 'results.html'} ({len(blocks)} queries).")
    publish_to_portfolio(page)
    if os.environ.get("SKIP_README") != "1":
        rewrite_readme(blocks, cfg)
        print(f"Refreshed output blocks in {ROOT / 'README.md'}.")


if __name__ == "__main__":
    main()
