#!/usr/bin/env python3
"""Run the dealflow data quality assertion suite and gate on the result.

WHAT THIS DOES
    Executes one battery of assertions from dq.assertion against one target
    star schema, prints a scannable PASS / FAIL / ERROR table with the
    offending row count for every assertion, and exits non-zero if any
    assertion of severity 'error' failed or could not run. That exit code is
    the whole point: it makes the suite usable as a pipeline gate or a CI
    step rather than something a human has to read and interpret.

WHY THE VERDICT LOGIC IS NOT IN HERE
    dq.fn_run_assertions in sql/07_quality.sql decides what an assertion
    MEANS and writes dq.assertion_result. This script only presents those
    results and turns them into an exit code. One implementation of the
    semantics, so psql and Python can never disagree about a verdict, and
    the suite stays fully usable from psql alone on a box with no Python.

WHY 'ERROR' IS NOT 'FAIL'
    An assertion that could not run has not passed. Treating the two alike
    is how a broken control reports green, so ERROR is reported separately
    and gates the exit code exactly like a failure.

CONNECTIVITY
    Prefers psycopg. Falls back to piping SQL through psql inside the
    Postgres container, so the suite runs on a machine with no Python
    database driver installed. The fallback is not a nicety: the gate has to
    work on whatever box the pipeline happens to run on.

USAGE
    ./run_quality.py --battery staging
    ./run_quality.py --battery warehouse --star mart
    ./run_quality.py --battery all --star perf --show-sample
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Sequence

REPO_ROOT = Path(__file__).resolve().parent
ENV_FILE = REPO_ROOT / '.env'
CONTAINER = os.environ.get('DEALFLOW_CONTAINER', 'dealflow-db')
DB_NAME = os.environ.get('POSTGRES_DB', 'dealflow')
DB_USER = os.environ.get('POSTGRES_USER', 'dealflow')

# A unit separator is used as the psql field delimiter instead of a comma or
# a pipe, because assertion names and business rationale contain both and a
# delimiter that can appear in the data is not a delimiter.
FIELD_SEP = '\x1f'


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
def load_env_file(path: Path) -> dict[str, str]:
    """Read a KEY=VALUE .env file. Values are never printed or logged."""
    values: dict[str, str] = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#') or '=' not in line:
            continue
        key, _, value = line.partition('=')
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


# ---------------------------------------------------------------------------
# Two ways to talk to the database, one interface
# ---------------------------------------------------------------------------
class Runner:
    """Executes SQL and returns rows as lists of strings."""

    def query(self, sql: str) -> list[list[str]]:  # pragma: no cover - interface
        raise NotImplementedError

    @property
    def label(self) -> str:  # pragma: no cover - interface
        raise NotImplementedError


class PsycopgRunner(Runner):
    def __init__(self, dsn_parts: dict[str, str]) -> None:
        import psycopg  # imported lazily so the psql fallback needs no driver

        self._conn = psycopg.connect(
            host=dsn_parts['host'], port=dsn_parts['port'],
            dbname=dsn_parts['dbname'], user=dsn_parts['user'],
            password=dsn_parts['password'], autocommit=True,
        )

    @property
    def label(self) -> str:
        return 'psycopg'

    def query(self, sql: str) -> list[list[str]]:
        with self._conn.cursor() as cur:
            cur.execute(sql)
            if cur.description is None:
                return []
            return [['' if v is None else _as_text(v) for v in row] for row in cur.fetchall()]


class PsqlRunner(Runner):
    """Runs SQL through psql inside the container. No Python driver needed."""

    def __init__(self, container: str, user: str, dbname: str, password: str) -> None:
        self._container = container
        self._user = user
        self._dbname = dbname
        self._password = password

    @property
    def label(self) -> str:
        return f'psql via docker exec {self._container}'

    def query(self, sql: str) -> list[list[str]]:
        cmd = [
            'docker', 'exec', '-i',
            '-e', 'PGPASSWORD',                  # value passed via env, never argv
            self._container, 'psql',
            '-U', self._user, '-d', self._dbname,
            '-v', 'ON_ERROR_STOP=1',
            '--no-align', '--tuples-only',
            '--field-separator', FIELD_SEP,
            '-c', sql,
        ]
        env = dict(os.environ, PGPASSWORD=self._password)
        proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if proc.returncode != 0:
            raise RuntimeError(f'psql failed: {proc.stderr.strip()}')
        rows: list[list[str]] = []
        for line in proc.stdout.splitlines():
            if not line.strip():
                continue
            rows.append(line.split(FIELD_SEP))
        return rows


def _as_text(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return str(value)


def build_runner(env: dict[str, str], force_psql: bool) -> Runner:
    password = env.get('POSTGRES_PASSWORD', '')
    if not password:
        raise SystemExit('POSTGRES_PASSWORD not found. Expected it in .env or the environment.')
    if not force_psql:
        try:
            return PsycopgRunner({
                'host': env.get('POSTGRES_HOST', 'localhost'),
                'port': env.get('POSTGRES_PORT', '5433'),
                'dbname': env.get('POSTGRES_DB', DB_NAME),
                'user': env.get('POSTGRES_USER', DB_USER),
                'password': password,
            })
        except Exception:
            # A missing driver or a refused TCP connection both mean the same
            # thing here: use the transport that is known to work.
            pass
    return PsqlRunner(CONTAINER, env.get('POSTGRES_USER', DB_USER),
                      env.get('POSTGRES_DB', DB_NAME), password)


# ---------------------------------------------------------------------------
# Presentation
# ---------------------------------------------------------------------------
STATUS_GLYPH = {'PASS': 'PASS', 'FAIL': 'FAIL', 'ERROR': 'ERR '}


def render_table(rows: Sequence[Sequence[str]], headers: Sequence[str],
                 aligns: Sequence[str]) -> str:
    widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], len(cell))

    def line(cells: Sequence[str]) -> str:
        out = []
        for i, cell in enumerate(cells):
            out.append(cell.rjust(widths[i]) if aligns[i] == 'r' else cell.ljust(widths[i]))
        return '  '.join(out).rstrip()

    sep = '  '.join('-' * w for w in widths)
    return '\n'.join([line(headers), sep] + [line(r) for r in rows])


def human_int(text: str) -> str:
    return f'{int(text):,}' if re.fullmatch(r'-?\d+', text or '') else (text or '-')


def band(lo: str, hi: str) -> str:
    return lo if lo == hi else f'{lo}..{hi}'


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Run the dealflow data quality assertion suite.')
    parser.add_argument('--battery', default='all', choices=['staging', 'warehouse', 'all'],
                        help="Which battery to run. 'staging' is a supplier scorecard and is "
                             "expected to report findings; 'warehouse' is the correctness "
                             "contract and must be clean.")
    parser.add_argument('--star', default='mart',
                        help='Star schema the warehouse assertions are pointed at (default mart).')
    parser.add_argument('--sample-rows', type=int, default=5,
                        help='Offending rows captured per failing assertion (default 5).')
    parser.add_argument('--show-sample', action='store_true',
                        help='Print the captured offending rows under the table.')
    parser.add_argument('--psql', action='store_true',
                        help='Force the psql transport instead of trying psycopg first.')
    args = parser.parse_args(argv)

    if not re.fullmatch(r'[a-z_][a-z0-9_]*', args.star):
        raise SystemExit(f'--star must be a plain schema identifier, got {args.star!r}')

    env = {**load_env_file(ENV_FILE), **{k: v for k, v in os.environ.items() if k.startswith('POSTGRES_')}}
    runner = build_runner(env, force_psql=args.psql)

    print()
    print('=' * 100)
    print(f'  DEALFLOW DATA QUALITY ASSERTION SUITE')
    print(f'  battery: {args.battery}    target star schema: {args.star}    transport: {runner.label}')
    print('=' * 100)

    # The database decides the verdicts. This script only reports them.
    run_id_rows = runner.query(
        f"SELECT dq.fn_run_assertions('{args.battery}', '{args.star}', {args.sample_rows})")
    if not run_id_rows:
        raise SystemExit('The runner function returned no assertion_run_id.')
    run_id = int(run_id_rows[0][0])

    results = runner.query(f"""
        SELECT res.assertion_code, res.severity, res.quality_dimension, res.status,
               coalesce(res.offending_rows::text, ''), res.expect_min::text,
               res.expect_max::text, round(res.duration_ms)::text,
               a.assertion_name, coalesce(res.error_text, ''),
               coalesce(res.offending_sample::text, '')
        FROM dq.assertion_result res
        JOIN dq.assertion a ON a.assertion_code = res.assertion_code
        WHERE res.assertion_run_id = {run_id}
        ORDER BY res.assertion_code
    """)

    table_rows: list[list[str]] = []
    failures: list[list[str]] = []
    passed = failed = errored = blocking = 0

    for (code, severity, dimension, status, offending, lo, hi,
         ms, name, error_text, sample) in results:
        if status == 'PASS':
            passed += 1
        elif status == 'FAIL':
            failed += 1
        else:
            errored += 1
        if status != 'PASS' and severity == 'error':
            blocking += 1

        table_rows.append([
            STATUS_GLYPH.get(status, status), code, severity, dimension,
            human_int(offending), band(lo, hi), ms, name[:58],
        ])
        if status != 'PASS':
            failures.append([code, name, status, human_int(offending),
                             band(lo, hi), error_text, sample])

    print()
    print(render_table(
        table_rows,
        headers=['', 'ASSERTION', 'SEV', 'DIMENSION', 'OFFENDING', 'EXPECT', 'MS', 'WHAT IT CHECKS'],
        aligns=['l', 'l', 'l', 'l', 'r', 'r', 'r', 'l'],
    ))

    print()
    print('-' * 100)
    print(f'  {len(results)} assertions run    {passed} pass    {failed} fail    {errored} error')
    print(f'  blocking (severity=error and not passing): {blocking}')

    by_dimension = runner.query(f"""
        SELECT res.quality_dimension,
               count(*)::text,
               count(*) FILTER (WHERE res.status <> 'PASS')::text
        FROM dq.assertion_result res
        WHERE res.assertion_run_id = {run_id}
        GROUP BY res.quality_dimension
        ORDER BY count(*) FILTER (WHERE res.status <> 'PASS') DESC, res.quality_dimension
    """)
    print()
    print('  Coverage by quality dimension:')
    print()
    print('    ' + render_table(
        [[d, human_int(t), human_int(f)] for d, t, f in by_dimension],
        headers=['DIMENSION', 'CHECKS', 'NOT PASSING'], aligns=['l', 'r', 'r'],
    ).replace('\n', '\n    '))

    if failures:
        print()
        print('-' * 100)
        print('  FINDINGS, most severe first')
        for code, name, status, offending, expect, error_text, sample in failures:
            print()
            print(f'  [{status}] {code}  {name}')
            print(f'         offending rows: {offending}   expected: {expect}')
            if error_text:
                print(f'         could not run: {error_text}')
            if args.show_sample and sample:
                try:
                    parsed = json.loads(sample)
                except (TypeError, ValueError):
                    parsed = None
                if parsed:
                    for example in parsed:
                        print(f'         e.g. {json.dumps(example, default=str, sort_keys=True)}')

    verdict = 'PASS' if blocking == 0 else 'FAIL'
    print()
    print('=' * 100)
    print(f'  GATE VERDICT: {verdict}    (assertion_run_id = {run_id})')
    print(f'  Full detail, including the captured offending rows, is in dq.assertion_result.')
    print('=' * 100)
    print()

    # Non-zero exit on any blocking failure, so this can gate a pipeline.
    return 0 if blocking == 0 else 1


if __name__ == '__main__':
    sys.exit(main())
