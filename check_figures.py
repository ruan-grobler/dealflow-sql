#!/usr/bin/env python3
"""Fail the build if a measured figure is restated outside the file that measured it.

WHY THIS EXISTS
    A reviewer found the same measurement written four different ways across four files:
    the planning cost of partition pruning appeared in README.md, INTERVIEW-NOTES.md and
    sql/02_warehouse.sql as "up to 21 ms against 0.2 ms", in PERFORMANCE.md as a different
    pair of numbers, and in the checked-in evidence file as a third. Nobody had lied. Somebody
    had re-measured, corrected one file, and left three copies behind.

THE RULE THIS ENFORCES
    A measured number appears ONCE, in the file that measured it. Everywhere else links to it.
    Where a second file is genuinely allowed to restate one, for instance the README's summary
    of the benchmark table, that permission is written down here rather than assumed.

    Retired figures are the other half. A number that was measured, found wrong and replaced
    must not come back, so each one is registered with the value that replaced it.

WHAT IT DELIBERATELY DOES NOT DO
    It does not parse prose or understand context. It is a grep with a registry, which is
    exactly as much machinery as the problem needs. Extending it means adding a line to
    FIGURES or RETIRED, not writing code.

    docs/defects.md is exempt from every rule, because documenting the number that used to be
    there is that file's entire job. plans/ and out/ are exempt because they are raw captured
    output, not prose: they are where figures come FROM.

USAGE
    python3 check_figures.py          # exit 1 on any violation
    ./run.sh figures                  # the same thing, from the build
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent

# Files that carry prose and are therefore in scope. plans/ and out/ hold raw psql output and
# are the source of these numbers rather than a restatement of them.
SCANNED = sorted(
    [p for p in ROOT.glob("*.md")]
    + [p for p in ROOT.glob("*.py") if p.name != "check_figures.py"]
    + [p for p in (ROOT / "sql").glob("*.sql")]
    + [p for p in (ROOT / "docs").glob("*.md")]
    + [ROOT / "run.sh"]
)

# The file whose job is to record what a number USED to be. Exempt from everything.
DEFECT_LOG = "docs/defects.md"

# ------------------------------------------------------------------------------------------
# LIVE FIGURES. Each one is measured in exactly one file. "echoes" lists the files that are
# allowed to restate it anyway, with the reason, because a summary that has to link out for
# every number is not a summary.
# ------------------------------------------------------------------------------------------
FIGURES: list[dict] = [
    {
        "figure": "8,200 kB",
        "what": "btree on raw.stage_event(ingested_at), the real landing table",
        "home": "PERFORMANCE.md",
        "echoes": {
            "README.md": "the one-line BRIN row in the techniques table",
            "INTERVIEW-NOTES.md": "the spoken version of the same answer",
        },
    },
    {
        "figure": "1,152,758",
        "what": "distinct ingested_at values in the lab mirror, which is why its btree is big",
        "home": "PERFORMANCE.md",
        "echoes": {"INTERVIEW-NOTES.md": "the spoken version of the same answer"},
    },
    {
        "figure": "8,527",
        "what": "ANALYZE on the partitioned parent, milliseconds",
        "home": "sql/06_performance.sql",
        "echoes": {},
    },
    {
        "figure": "1,127,596",
        "what": "case 3 after-plan shared buffers",
        "home": "PERFORMANCE.md",
        "echoes": {"INTERVIEW-NOTES.md": "the spoken version of the case 3 buffer trade"},
    },
]

# ------------------------------------------------------------------------------------------
# RETIRED FIGURES. Measured once, found wrong, replaced. If one of these strings reappears in
# a scanned file, something was copied from an old revision.
# ------------------------------------------------------------------------------------------
RETIRED: list[dict] = [
    {
        "figure": "up to 21 ms",
        "was": "partition planning cost with no pruning",
        "now": "the measured range in PERFORMANCE.md, 'The cost side of partitioning'",
    },
    {
        "figure": "1,144,830",
        "was": "the event fact row count",
        "now": "1,165,043",
    },
    {
        "figure": "43,643",
        "was": "Q09 suppressed re-entries",
        "now": "95,550",
    },
    {
        "figure": "2,704",
        "was": "Q12 island count",
        "now": "7,801 islands over 800 named brokers",
    },
    {
        "figure": "1,439,009 rows",
        "was": "the stg row count in the architecture diagram, which omitted both dimension feeds",
        "now": "1,514,310, with the transaction split shown beneath it",
    },
    {
        "figure": "log10 min 4.70",
        "was": "the asserted deal value range in Q07",
        "now": "log10 min 2.52, log10 max 9.61",
    },
    {
        "figure": "3,293 ms",
        "was": "quoted as the cost of adding one foreign key to the partitioned parent",
        "now": "117 to 344 ms for one key; 3,293 ms was closer to the cost of all five",
    },
    {
        "figure": "16.9x smaller",
        "was": "the partial index size ratio",
        "now": "16.5x, from plans/evidence_820_partial_index_size.txt",
    },
]


def relative(path: Path) -> str:
    return str(path.relative_to(ROOT))


def main() -> int:
    violations: list[str] = []

    for entry in FIGURES:
        needle = entry["figure"]
        allowed = {entry["home"], DEFECT_LOG} | set(entry["echoes"])
        for path in SCANNED:
            name = relative(path)
            if name in allowed:
                continue
            if needle in path.read_text(encoding="utf-8", errors="replace"):
                violations.append(
                    f"{name}: restates \"{needle}\" ({entry['what']}).\n"
                    f"    That figure is measured in {entry['home']}. Link to it instead, or "
                    f"add {name} to its echoes in check_figures.py with a reason."
                )

    for entry in RETIRED:
        needle = entry["figure"]
        for path in SCANNED:
            name = relative(path)
            if name == DEFECT_LOG:
                continue
            if needle in path.read_text(encoding="utf-8", errors="replace"):
                violations.append(
                    f"{name}: contains the RETIRED figure \"{needle}\" ({entry['was']}).\n"
                    f"    It was replaced by: {entry['now']}."
                )

    if violations:
        print(f"check_figures: {len(violations)} violation(s)\n")
        for v in violations:
            print(f"  {v}\n")
        print("A measured number lives in one file. Everywhere else is a link.")
        return 1

    print(
        f"check_figures: OK. {len(FIGURES)} single-source figures and "
        f"{len(RETIRED)} retired figures checked across {len(SCANNED)} files."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
