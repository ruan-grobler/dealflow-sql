#!/usr/bin/env python3
"""DealFlow warehouse synthetic data generator (raw landing zone).

WHAT THIS PRODUCES
    Four append-only raw landing tables, every source column text, plus ingest
    metadata. These are the replay point of the warehouse: staging, intermediate
    and mart layers are all rebuildable from here without re-fetching anything.

        raw.deal_submission   one row per inbound broker deal submission as received
        raw.stage_event       one row per deal stage transition payload as received
        raw.broker_directory  one row per broker per monthly full directory snapshot
        raw.property_register one row per property register event (initial + delta)

    ALL DATA IS SYNTHETIC. Emails use .example domains. Brokers are "Broker
    BRK-000123", agencies are "Agency 041", suburbs and metros are invented
    compounds, properties are "Erf 1123 <invented suburb>". The only real-world
    strings in the dataset are the nine South African province names, which are
    public administrative geography, not people or companies. No real broker,
    agency, deal, client, colleague or employer appears anywhere.

DETERMINISM CONTRACT (the reason this file has no random.random() at module level)
    Seed: DEALFLOW_SEED = "dealflow-2026-v1" (change it and you get a different
    but equally reproducible dataset).

    Every random draw is a pure function of (seed, namespace, row key). Nothing
    depends on iteration order, insertion order, thread count or how many rows
    were generated before it, so any single row can be regenerated in isolation
    and two machines produce byte-identical files.

        u(ns, key)    -> uniform [0,1), one draw
        rng(ns, key)  -> random.Random seeded from (seed, ns, key), many draws

    u() is deliberately byte-compatible with the warehouse SQL helper
    util.fn_u(p_seed, p_key), which is defined as
        ('x' || substr(md5(p_seed || p_key), 1, 8))::bit(32)::bigint / 4294967296.0
    Calling util.fn_u('dealflow-2026-v1|value|', 'DL-202403-000117') in Postgres
    returns exactly what u('value', 'DL-202403-000117') returns here. That parity
    is asserted by --self-check so the Python generator and the SQL layer can
    never silently disagree.

    WHY NOT setseed()/random(): a parallel plan seeds a PRNG per worker, so the
    same seed produces different data run to run. Hash-derived draws are immune
    because they never depend on plan shape.

    Exact-count selections (the 1,000 designated parse losses, the 30 NULL-sector
    properties, the 12 duplicated directory rows) use pick_exact(), which takes
    the N smallest hash scores. Exact and still order independent.

VOLUME (at --scale 1.0)
    60 agencies, 800 brokers (760 present in the first snapshot, 40 later joiners),
    12,000 properties, 79 monthly directory snapshots (2020-01 .. 2026-07),
    250,000 canonical deals, about 1.15M accepted stage events,
    history 2020-01-01 .. 2026-07-31 (the reporting cut).

SHAPE (measured, not assumed. --report prints the achieved figures from SQL)
    Power-law broker productivity: broker of productivity rank r gets weight
        r ** -0.75, which puts the top decile of brokers on about 48% of deals and
        the bottom decile on about 3%. Both are printed by --report.
    Log-normal deal value per sector, median R8.6m (Land) to R54.1m (Mixed use),
        sigma 1.00 to 1.15, so the mean sits well above the median in every sector.
    Seasonality: February and March peak near 1.4x, December collapses to about
        0.22x the monthly average (the South African property market's real dead zone).
    Growth: 10% compound annual growth in submissions.
    Leaking funnel: about 91% Triaged, 62% Qualified, 39% Underwriting, 25% Offer,
        terminating about 7% won, 50% lost, 39% stalled, the rest still open at the cut.
    Stall threshold is drawn per deal from Normal(mean 90 days, sd 15, floor 45),
        never a hard cutoff, so the dwell distribution has no spike at exactly 90.00
        days. A fixed cutoff is an obvious generator fingerprint.
    days_in_prev_stage is derivable for EVERY transition including terminal ones,
        so average time in stage is not NULL for Lost and Stalled, which are the two
        most interesting stages in the funnel.
    Source channel mix is deliberately skewed (Email intake 46%, Portal 27%,
        Referral 14%, Direct call 9%, Auction notice 4%). A near-uniform five-way
        split is a generator fingerprint no real intake mix produces.

DIRTY DATA INJECTION RATES (assert against these; --report prints achieved counts)
    On raw.deal_submission (base 250,000 canonical rows + duplicates):
        D01  5.00%  of canonical deals are re-submitted as a duplicate row: new
                    deal reference, new message id, same broker + property + ISO
                    week, value identical (40%) or jittered up to 2% (60%).
        D02  1.20%  of rows: deal value absent entirely (NULL).
        D03  0.40%  of rows: value captured in thousands, not units (value / 1000).
        D04  0.60%  of rows: value unparseable text ("R 42 mil", "TBC", ...).
        D05  0.90%  of rows: received timestamp impossible or unparseable
                    ("2026-02-30", "31/02/2025", "yesterday").
        D06  1.50%  of rows: broker email case and whitespace variants.
        D07  0.80%  of rows: broker reference not present in the directory.
        D08  0.50%  of rows: property reference absent, property name only, with a
                    trailing " (Pty) Ltd" variant that must be fuzzy conformed.
        D09  0.30%  of rows: negative value (credit note typo).
        D10  1.00%  of rows: broker-typed sector hint with case and whitespace noise.
    On raw.stage_event (base about 1.15M accepted rows):
        E01  1.90%  of accepted events are redelivered verbatim as an extra row.
        E02  6,000  extra rows: physically impossible transitions (out of a terminal
                    stage, or backwards three or more stages).
        E03  2,000  extra rows: unknown stage code.
        E04  0.10%  extra rows: events for a deal reference never submitted.
        E05  0.30%  of accepted events are mutated so event_ts precedes the deal's
                    received_ts (clock or capture error).
        E06  0.20%  of accepted events are mutated to a date outside the fact's
                    partition range (2019 or 2031).
    On raw.broker_directory (79 snapshots):
        B01  12 rows exactly: a broker listed twice in one snapshot.
        B02  1.00%  of rows: email case and whitespace variants.
        B03  0.80%  of rows: region and tier case and whitespace variants. These
                    matter more than they look: if staging does not conform them,
                    the SCD2 row hash changes and a spurious dimension version opens.
    On raw.property_register (12,000 initial + 1,600 delta):
        P01  30 rows exactly: NULL sector in the initial load.
        P02  1.00%  of rows: sector case and whitespace variants.

THE ONE HEADLINE INVARIANT, MADE ASSERTABLE
    Exactly 1,000 canonical deals are designated total parse losses: their only
    submission row is unparseable (D04 or D05) and they emit no stage events,
    because a deal whose intake failed was never created in the pipeline system.
    Every other unparseable row is placed on the DUPLICATE row of a duplicate
    pair, never on the original, so it always has a parseable twin. Therefore:

        250,000 canonical deals - 1,000 total parse losses = 249,000 deals that
        the pipeline system created and that emitted at least one stage event.

    What that guarantees, exactly, and what it does not:
        IT DOES guarantee that the feed contains 249,000 canonical deal
            identities, each with a parseable submission row and a real funnel.
        IT DOES NOT by itself guarantee 249,000 rows in int.deal_deduped. That
            equality also needs the dedupe step to collapse every one of the
            12,500 resubmissions onto its original, and MEASURED, the dedupe
            fingerprint agreed in the design does not manage that. See the next
            section. A generator that quietly claimed the equality would be
            handing the warehouse builder a broken assertion.
    --report prints every count in the ledger from live COUNT(*), never from a
    constant in this file, so the ledger cannot drift from the data.

MEASURED FINDING THE WAREHOUSE BUILDER MUST ACT ON: THE DEDUPE FINGERPRINT
    The agreed intermediate-layer fingerprint is
        (broker_nk, property_nk, round(value / 100000), date_trunc('week', received_ts))
    That absolute R100,000 value bucket does not survive contact with this data,
    because a duplicate resubmission carries the value jittered by up to 2% and
    2% of a R28.9m deal is 5.8 buckets wide. Scored on the loaded feed, against
    the 8,920 duplicate pairs that are visible in the parseable set:

        value bucket rule                       caught  missed  false merges
        round(value / 100000)   (agreed)         4,543   4,377        4
        round(log(value)/log(1.05))  5pct bands  7,843   1,077       45
        round(log(value)/log(1.10)) 10pct bands  8,372     548       58
        round(log(value)/log(1.25)) 25pct bands  8,686     234       79
        no value in the key at all               8,920       0      405
        no value in the key, 3pct tolerance      8,920       0       68

    "False merges" are groups holding two genuinely different deals that the rule
    would wrongly collapse into one. Read the table as: the agreed fingerprint
    misses 49% of the duplicates it exists to catch, and no absolute bucket can
    fix that, because the defect is proportional and the bucket is not.

    RECOMMENDATION, and the numbers above are the argument for it: key the
    fingerprint on (broker_nk, property_nk, date_trunc('week', received_ts)) with
    NO value term, then inside each group suppress a row only when its value is
    within 3% of the row being kept. That catches all 8,920 with 68 false merges,
    0.03% of the 244,143 groups. It beats every bucket because a bucket always
    fails at its boundary and a tolerance test has no boundary to fail at.
    The dedupe_candidates probe in --report recomputes this whole table from live
    data on every run, so the recommendation stays measured rather than becoming
    a comment that used to be true.

TWO CONTRACTS THE DOWNSTREAM LAYERS MUST HONOUR (they are not optional)
    1. Orphan-deal events (E04), out-of-order events (E05) and impossible
       transitions (E02) must be quarantined in the intermediate layer so they
       never reach the fact. Otherwise the blocking DQ gates fail on the seeded
       demo data, which is worse than having no demo.
    2. Out-of-range event dates (E06) land in the fact's DEFAULT partition on
       purpose, so the DEFAULT partition check must be a warn with an expected
       non-zero band, not an error asserting empty.

USAGE
    python3 generate_data.py --schema raw_gen                 # generate and load
    python3 generate_data.py --schema raw_gen --truncate      # rebuild it from empty
    python3 generate_data.py --schema raw_gen --scale 0.02    # fast smoke test
    python3 generate_data.py --self-check                     # determinism checks only
    python3 generate_data.py --schema raw_gen --report-only   # re-print the ledger

    The landing tables are append only, so a load into a schema that already
    holds rows is refused rather than allowed to double the dataset silently.
    Pass --truncate to rebuild from empty, or --append if a second copy is
    genuinely what you want.

    Loads via psql COPY ... FROM STDIN inside the Postgres container, never
    row-by-row INSERT. The password is read from .env and passed through the
    environment; it is never printed and never written to a file.
"""

from __future__ import annotations

import argparse
import heapq
import json
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, Sequence

# ---------------------------------------------------------------------------
# 0. Determinism primitives
# ---------------------------------------------------------------------------

DEALFLOW_SEED = "dealflow-2026-v1"

# md5 is used as a fast, stable, cross-platform bit source, not as a security
# primitive. It has to stay md5 because the SQL side (util.fn_u) uses md5, and
# the two must agree byte for byte.
from hashlib import md5  # noqa: E402  (imported here so the reason above reads first)

_TWO32 = 4294967296.0


def u(namespace: str, key: str) -> float:
    """Uniform [0,1) that is a pure function of (seed, namespace, key)."""
    digest = md5(f"{DEALFLOW_SEED}|{namespace}|{key}".encode()).hexdigest()
    return int(digest[:8], 16) / _TWO32


def rng(namespace: str, key: str) -> random.Random:
    """A private PRNG stream for one row, seeded the same way u() is.

    Used where a row needs many correlated draws (a deal's whole funnel walk).
    Seeding per row costs a few microseconds and buys complete independence from
    iteration order, which is what makes the dataset reproducible.
    """
    return random.Random(md5(f"{DEALFLOW_SEED}|{namespace}|{key}".encode()).digest())


def pick_exact(keys: Sequence[str], namespace: str, n: int) -> set[str]:
    """Deterministically choose EXACTLY n keys, order independently.

    Rate-based selection ("take it if u < p") gives an approximate count, which
    is fine for noise but useless for an invariant somebody has to assert on.
    Taking the n smallest hash scores is exact and still order independent.
    """
    if n <= 0:
        return set()
    scored = ((u(namespace, k), k) for k in keys)
    return {k for _, k in heapq.nsmallest(n, scored)}


def weighted_index(cum: Sequence[float], x: float) -> int:
    """Index for a draw x in [0,1) against a normalised cumulative weight array."""
    lo, hi = 0, len(cum) - 1
    target = x * cum[-1]
    while lo < hi:
        mid = (lo + hi) // 2
        if cum[mid] < target:
            lo = mid + 1
        else:
            hi = mid
    return lo


def lognormal(r: random.Random, median_days: float, sigma: float = 0.85) -> float:
    """Days drawn log-normally around a median. Floor keeps events strictly ordered."""
    return max(0.30, median_days * math.exp(sigma * r.gauss(0.0, 1.0)))


# ---------------------------------------------------------------------------
# 1. Reference data. Every name here is invented except the provinces.
# ---------------------------------------------------------------------------

PROVINCES = [
    "Gauteng",
    "Western Cape",
    "KwaZulu-Natal",
    "Eastern Cape",
    "Free State",
    "Mpumalanga",
    "Limpopo",
    "North West",
    "Northern Cape",
]
PROVINCE_WEIGHTS = [0.34, 0.22, 0.15, 0.07, 0.05, 0.06, 0.05, 0.04, 0.02]

# Invented metros. Deliberately not the names of real municipalities.
METROS = [
    "Hoeveld Metro",
    "Kaapstrand Metro",
    "Baaistad Metro",
    "Goudveld Metro",
    "Vaaldrif Metro",
    "Suikerbaai Metro",
    "Wesrand Metro",
    "Noordveld Metro",
]

# Invented suburb compounds. Checked by eye against real South African place
# names so that none of them is a real suburb.
SUBURBS = [
    "Kleinrand", "Vaalhoek", "Brakspruit", "Duinfontein", "Wildekloof",
    "Skuinsbaai", "Perdedal", "Melkkop", "Aalwynpark", "Bokveldrand",
    "Sonkyk", "Windvlakte", "Klipperand", "Sederhoogte", "Naaldberg",
    "Kruidfontein", "Olyfkloof", "Doringlaagte", "Renosterbaai", "Kwaggadal",
    "Seekoeivlakte", "Boomrand", "Grysrivier", "Swartdoring", "Nuwehoek",
    "Ystervarkkop", "Malmokbaai", "Kareelaagte", "Soutpanrand", "Wilgerhoek",
    "Aloe Ridge", "Ironstone Park", "Quarry View", "Sandalwood Heights",
    "Copperfield North", "Marabou Ridge", "Tamboti Park", "Saltbush View",
    "Fernkloof Heights", "Baobab Rise",
]

SECTORS: dict[str, dict] = {
    # median_zar drives the log-normal centre, sigma its spread. The spread is
    # what makes the mean sit far above the median, which is how commercial
    # property values actually behave.
    "Industrial":  {"w": 0.22, "median_zar": 15_900_000, "sigma": 1.05,
                    "subs": ["Warehouse", "Light manufacturing", "Logistics park", "Mini units"]},
    "Retail":      {"w": 0.18, "median_zar": 28_900_000, "sigma": 1.10,
                    "subs": ["Neighbourhood centre", "Strip mall", "Regional centre", "Standalone store"]},
    "Office":      {"w": 0.17, "median_zar": 23_000_000, "sigma": 1.10,
                    "subs": ["Single tenant", "Multi tenant", "Business park", "Serviced suites"]},
    "Residential": {"w": 0.14, "median_zar": 38_100_000, "sigma": 1.05,
                    "subs": ["Sectional title block", "Student accommodation", "Rental portfolio"]},
    "Land":        {"w": 0.12, "median_zar": 8_600_000, "sigma": 1.15,
                    "subs": ["Serviced erf", "Agricultural holding", "Development bulk"]},
    "Mixed use":   {"w": 0.07, "median_zar": 54_100_000, "sigma": 1.15,
                    "subs": ["Retail over residential", "Office over retail", "Precinct"]},
    "Hospitality": {"w": 0.06, "median_zar": 31_000_000, "sigma": 1.05,
                    "subs": ["Guest lodge", "Hotel", "Conference venue"]},
    "Healthcare":  {"w": 0.04, "median_zar": 25_800_000, "sigma": 1.00,
                    "subs": ["Day clinic", "Consulting suites", "Sub acute facility"]},
}
SECTOR_NAMES = list(SECTORS)
SECTOR_CUM: list[float] = []
_acc = 0.0
for _s in SECTOR_NAMES:
    _acc += SECTORS[_s]["w"]
    SECTOR_CUM.append(_acc)

GRADES = ["P", "A", "B", "C"]
GLA_BANDS = ["<1000", "1000-2500", "2500-5000", "5000-10000", "10000-25000", "25000+"]
GLA_MIDPOINTS = [600, 1700, 3600, 7200, 16000, 42000]

REGIONS = ["Inland North", "Inland South", "Coastal West", "Coastal East", "Interior"]
BROKER_TIERS = ["Principal", "Senior", "Mid", "Junior"]
BROKER_TIER_WEIGHTS = [0.08, 0.27, 0.40, 0.25]
AGENCY_TIERS = ["National", "Regional", "Boutique"]
HEADCOUNT_BANDS = ["1-5", "6-15", "16-40", "41-100", "100+"]

CHANNELS = ["Email intake", "Portal", "Referral", "Direct call", "Auction notice"]
CHANNEL_WEIGHTS = [0.46, 0.27, 0.14, 0.09, 0.04]
CHANNEL_CUM: list[float] = []
_acc = 0.0
for _w in CHANNEL_WEIGHTS:
    _acc += _w
    CHANNEL_CUM.append(_acc)

# Stage 1..8 are the forward path, 9 and 10 are the other two terminal stages.
STAGE_CODES = {
    1: "RECEIVED",
    2: "TRIAGED",
    3: "QUALIFIED",
    4: "UNDERWRITING",
    5: "OFFER",
    6: "DUE_DILIGENCE",
    7: "LEGAL",
    8: "CLOSED_WON",
    9: "LOST",
    10: "STALLED",
}
UNKNOWN_STAGE_CODES = ["REVIEW", "PENDING_INFO", "STG-99", "ON HOLD"]

# Per stage: probability the deal stalls, probability it is lost, and the median
# dwell in days before the next forward move. advance = 1 - stall - lost.
# The keep rates (advance) are chosen to reproduce the target funnel:
#   91% Triaged, 62% Qualified, 39% Underwriting, 25% Offer, 7% won overall.
STAGE_P: dict[int, tuple[float, float, float]] = {
    1: (0.054, 0.038, 2.5),    # Received     -> keep 0.908
    2: (0.190, 0.132, 8.0),    # Triaged      -> keep 0.678
    3: (0.155, 0.205, 18.0),   # Qualified    -> keep 0.640
    4: (0.120, 0.248, 26.0),   # Underwriting -> keep 0.632
    5: (0.100, 0.280, 16.0),   # Offer        -> keep 0.620
    6: (0.080, 0.270, 32.0),   # Due dilig.   -> keep 0.650
    7: (0.040, 0.260, 26.0),   # Legal        -> keep 0.700 (the only path to won)
}
LOST_DWELL_MULTIPLIER = 1.30   # a loss decision takes longer than a clean advance
CHURN_P = 0.30                 # one legal single-step backward move, then forward again
VALUE_REVISION_P = 0.12        # share of events that carry a revised deal value

MONTH_SEASONALITY = {
    1: 0.78, 2: 1.38, 3: 1.42, 4: 1.02, 5: 1.06, 6: 0.95,
    7: 1.00, 8: 1.05, 9: 1.08, 10: 1.10, 11: 1.02, 12: 0.22,
}
ANNUAL_GROWTH = 1.10

HISTORY_START = date(2020, 1, 1)
HISTORY_END = date(2026, 7, 31)       # the reporting cut
# The cut is the END of the last day of history, not close of business on it.
# It has to be, because a submission is received between 06:00 and 19:00 while
# events only ever land inside working hours. With the cut at 18:00 the RECEIVED
# event of a deal submitted at 18:30 on the last day was trimmed away, that deal
# emitted no events at all, and the headline invariant came out at 248,988
# instead of 249,000. MEASURED: exactly 12 deals fell through that gap.
CUT_TS = datetime(2026, 7, 31, 23, 59, 59)

BROKER_POWER_LAW_BETA = 0.75          # measured: top decile about 48% of deals
PROPERTY_POWER_LAW_BETA = 0.55
IDLE_PROPERTY_SHARE = 0.08            # properties deliberately given zero deals

# Injection rates. Names match the header table so an assertion can quote them.
RATE = {
    "D01_duplicate": 0.0500,
    "D02_value_null": 0.0120,
    "D03_value_thousands": 0.0040,
    "D04_value_unparseable": 0.0060,
    "D05_date_unparseable": 0.0090,
    "D06_email_variant": 0.0150,
    "D07_unknown_broker": 0.0080,
    "D08_property_name_only": 0.0050,
    "D09_negative_value": 0.0030,
    "D10_sector_hint_noise": 0.0100,
    "E01_event_redelivered": 0.0190,
    "E04_orphan_event": 0.0010,
    "E05_event_before_receipt": 0.0030,
    "E06_event_out_of_range": 0.0020,
    "B02_directory_email_variant": 0.0100,
    "B03_directory_text_variant": 0.0080,
    "P02_register_sector_variant": 0.0100,
}
# Exact counts, quoted at --scale 1.0. Anything whose population scales with the
# deal or property volume is scaled with it by scaled_count(), so a smoke test at
# scale 0.02 does not end up with 20% of its deals designated as parse losses.
# B01 does not scale, because the broker count does not scale.
COUNT = {
    "designated_parse_losses": 1000,
    "E02_impossible_transitions": 6000,
    "E03_unknown_stage_codes": 2000,
    "B01_duplicate_directory_rows": 12,
    "P01_null_sector_rows": 30,
}


def scaled_count(key: str, scale: float) -> int:
    return max(1, round(COUNT[key] * scale))

UNPARSEABLE_VALUES = ["R 42 mil", "TBC", "see attached", "1,2 miljoen", "R42m ono", "n/a"]
UNPARSEABLE_DATES = ["2026-02-30", "31/02/2025", "yesterday", "2025-13-04", "0000-00-00"]
NOTE_SNIPPETS = [
    "Mandate signed, seller motivated",
    "Off market opportunity, confidential",
    "Tenant schedule to follow",
    "Zoning certificate requested",
    "Buyer pre approved",
    "Seller wants a quick transfer",
    "Municipal clearance outstanding",
    "Valuation instructed",
]


# ---------------------------------------------------------------------------
# 2. Postgres COPY text format writer
# ---------------------------------------------------------------------------

# COPY text format needs exactly four escapes. Everything else, including the
# leading and trailing spaces the dirty-data classes inject, is passed through
# byte for byte, which is what makes the whitespace defects survive the load.
_COPY_ESCAPES = str.maketrans({"\\": "\\\\", "\t": "\\t", "\n": "\\n", "\r": "\\r"})


def copy_line(values: Iterable[object]) -> str:
    out = []
    for v in values:
        if v is None:
            out.append("\\N")
        else:
            out.append(str(v).translate(_COPY_ESCAPES))
    return "\t".join(out) + "\n"


def ts_text(dt: datetime) -> str:
    """Explicit +02:00 offset so a raw value never depends on session timezone."""
    return dt.strftime("%Y-%m-%d %H:%M:%S") + "+02"


def money_text(v: float) -> str:
    return f"{v:.2f}"


def shift_year(dt: datetime, year: int) -> datetime:
    """Move a timestamp to another year, clamping the day of month.

    datetime.replace(year=...) raises on 29 February moving into a common year,
    which is exactly the case the out-of-range date defect hits: the generator
    crashed on a real 2024-02-29 event before this was added. Clamping is the
    right behaviour for a mistyped year, because that is what a human retyping
    a date produces.
    """
    day = min(dt.day, days_in_month(date(year, dt.month, 1)))
    return dt.replace(year=year, day=day)


def payload_hash(*parts: object) -> str:
    return md5("|".join("" if p is None else str(p) for p in parts).encode()).hexdigest()


def case_whitespace_noise(text: str, r: random.Random) -> str:
    """Case and whitespace mangling of the kind a human keyboard produces."""
    choice = r.random()
    if choice < 0.34:
        return "  " + text.upper()
    if choice < 0.67:
        return text.lower() + "   "
    return " " + text.title().replace(" ", "  ") + " "


# ---------------------------------------------------------------------------
# 3. Entity builders
# ---------------------------------------------------------------------------


@dataclass
class Agency:
    code: str
    name: str
    tier: str
    province: str
    is_national: bool
    headcount_band: str


@dataclass
class Broker:
    nk: str
    full_name: str
    email: str
    phone: str
    join_snapshot: int              # index into the snapshot list
    agency_idx: int
    region: str
    tier: str
    productivity_rank: int          # 1 = most productive
    # (snapshot_index, attribute, new_value) applied in order
    changes: list[tuple[int, str, object]] = field(default_factory=list)


@dataclass
class Property:
    nk: str
    name: str
    suburb: str
    metro: str
    province: str
    sector: str | None
    sub_sector: str
    grade: str
    gla_sqm: int
    gla_band: str
    weight: float


def build_agencies(n: int) -> list[Agency]:
    agencies: list[Agency] = []
    for i in range(1, n + 1):
        code = f"AGC-{i:03d}"
        r = rng("agency", code)
        tier = AGENCY_TIERS[weighted_index([0.20, 0.65, 1.00], r.random())]
        agencies.append(
            Agency(
                code=code,
                name=f"Agency {i:03d}",
                tier=tier,
                province=PROVINCES[weighted_index(_cum(PROVINCE_WEIGHTS), r.random())],
                is_national=(tier == "National"),
                headcount_band=HEADCOUNT_BANDS[
                    weighted_index([0.15, 0.45, 0.75, 0.93, 1.00], r.random())
                ],
            )
        )
    return agencies


def _cum(weights: Sequence[float]) -> list[float]:
    out, acc = [], 0.0
    for w in weights:
        acc += w
        out.append(acc)
    return out


def build_brokers(n: int, n_agencies: int, n_snapshots: int) -> list[Broker]:
    """Brokers plus their attribute-change history over the snapshot window.

    About 45% of brokers change at least one tracked attribute (agency, region,
    tier, is_active) at some point, giving roughly 1.5 SCD2 versions per broker.
    That is what makes the Type 2 dimension worth having: an agency-level number
    for 2022 must use the agency the broker was at in 2022, not today's.
    """
    brokers: list[Broker] = []
    # Productivity rank is a stable property of the broker, assigned by shuffling
    # a hash-derived score, so it does not correlate with the broker number.
    ranked = sorted(range(1, n + 1), key=lambda i: u("productivity", f"BRK-{i:06d}"))
    rank_of = {i: pos + 1 for pos, i in enumerate(ranked)}

    n_joiners = max(1, round(n * 0.05))
    joiner_ids = pick_exact([f"BRK-{i:06d}" for i in range(1, n + 1)], "joiner", n_joiners)

    for i in range(1, n + 1):
        nk = f"BRK-{i:06d}"
        r = rng("broker", nk)
        join_snapshot = 0
        if nk in joiner_ids:
            # Later joiners exist so the SCD2 anchoring rule gets exercised both
            # ways: first-snapshot keys anchor at -infinity, real joiners anchor
            # at their first snapshot date.
            join_snapshot = 1 + int(r.random() * (n_snapshots - 6))
        tier = BROKER_TIERS[weighted_index(_cum(BROKER_TIER_WEIGHTS), r.random())]
        broker = Broker(
            nk=nk,
            full_name=f"Broker {nk}",
            email=f"{nk.lower()}@agency{1 + int(r.random() * n_agencies):03d}.example",
            phone=f"+27 {10 + int(r.random() * 79)} {100 + int(r.random() * 899)} {1000 + int(r.random() * 8999)}",
            join_snapshot=join_snapshot,
            agency_idx=int(r.random() * n_agencies),
            region=REGIONS[int(r.random() * len(REGIONS))],
            tier=tier,
            productivity_rank=rank_of[i],
        )

        # 55% never change, 35% change once, 9% twice, 1% three times.
        x = r.random()
        n_changes = 0 if x < 0.55 else 1 if x < 0.90 else 2 if x < 0.99 else 3
        used: set[int] = set()
        for c in range(n_changes):
            span_lo = broker.join_snapshot + 3
            if span_lo >= n_snapshots - 1:
                break
            snap = span_lo + int(r.random() * (n_snapshots - 1 - span_lo))
            if snap in used:
                continue
            used.add(snap)
            kind = r.random()
            if kind < 0.40:
                broker.changes.append((snap, "agency_idx", int(r.random() * n_agencies)))
            elif kind < 0.70:
                # A promotion moves up one tier, which is how tiers actually move.
                cur = BROKER_TIERS.index(broker.tier)
                broker.changes.append((snap, "tier", BROKER_TIERS[max(0, cur - 1)]))
            elif kind < 0.85:
                broker.changes.append((snap, "region", REGIONS[int(r.random() * len(REGIONS))]))
            else:
                broker.changes.append((snap, "is_active", False))
        broker.changes.sort()

        # A handful of Type 1 corrections: a name spelling fix must NOT open a
        # new dimension version, and the only way to prove that is to seed one.
        if r.random() < 0.025:
            snap = min(n_snapshots - 1, broker.join_snapshot + 6 + int(r.random() * 20))
            broker.changes.append((snap, "full_name", f"Broker {nk} (corrected)"))
            broker.changes.sort()

        brokers.append(broker)
    return brokers


def build_properties(n: int, scale: float = 1.0) -> list[Property]:
    props: list[Property] = []
    null_sector_ids = pick_exact(
        [f"PRP-{i:06d}" for i in range(1, n + 1)], "null_sector",
        min(scaled_count("P01_null_sector_rows", scale), n),
    )
    idle_ids = pick_exact(
        [f"PRP-{i:06d}" for i in range(1, n + 1)], "idle_property",
        round(n * IDLE_PROPERTY_SHARE),
    )
    # Property trading frequency is skewed as well, and a deliberate slice of the
    # estate never trades at all so the "nothing live against it" coverage query
    # has something real to find.
    ranked = sorted(range(1, n + 1), key=lambda i: u("prop_rank", f"PRP-{i:06d}"))
    rank_of = {i: pos + 1 for pos, i in enumerate(ranked)}

    for i in range(1, n + 1):
        nk = f"PRP-{i:06d}"
        r = rng("property", nk)
        sector = SECTOR_NAMES[weighted_index(SECTOR_CUM, r.random())]
        band_idx = weighted_index(_cum([0.18, 0.24, 0.22, 0.18, 0.13, 0.05]), r.random())
        grade = GRADES[weighted_index(_cum([0.08, 0.34, 0.38, 0.20]), r.random())]
        if sector in ("Land", "Industrial") and grade == "P":
            grade = "A"
        suburb = SUBURBS[int(r.random() * len(SUBURBS))]
        props.append(
            Property(
                nk=nk,
                name=f"Erf {1000 + int(r.random() * 8999)} {suburb}",
                suburb=suburb,
                metro=METROS[int(r.random() * len(METROS))],
                province=PROVINCES[weighted_index(_cum(PROVINCE_WEIGHTS), r.random())],
                sector=None if nk in null_sector_ids else sector,
                sub_sector=SECTORS[sector]["subs"][int(r.random() * len(SECTORS[sector]["subs"]))],
                grade=grade,
                gla_sqm=int(GLA_MIDPOINTS[band_idx] * (0.6 + 0.8 * r.random())),
                gla_band=GLA_BANDS[band_idx],
                weight=0.0 if nk in idle_ids else rank_of[i] ** -PROPERTY_POWER_LAW_BETA,
            )
        )
    return props


def month_starts(start: date, end: date) -> list[date]:
    out, y, m = [], start.year, start.month
    while date(y, m, 1) <= end:
        out.append(date(y, m, 1))
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return out


# ---------------------------------------------------------------------------
# 4. Feed writers: broker directory and property register
# ---------------------------------------------------------------------------


def write_broker_directory(
    path: Path, brokers: list[Broker], agencies: list[Agency], snapshots: list[date]
) -> int:
    """Full monthly snapshots. A full snapshot is what makes hash-compare SCD2
    change detection the right technique: you cannot trust a delta you never got.
    """
    rows = 0
    dup_targets = pick_exact(
        [b.nk for b in brokers], "dir_dup", COUNT["B01_duplicate_directory_rows"]
    )
    with path.open("w", encoding="utf-8") as fh:
        for snap_idx, snap in enumerate(snapshots):
            ingest_run = f"ING-DIR-{snap:%Y-%m}"
            ingested_at = ts_text(datetime.combine(snap, datetime.min.time()) + timedelta(hours=6))
            source_file = f"broker_directory_{snap:%Y_%m}.csv"
            line_no = 0
            buf: list[str] = []
            for b in brokers:
                if snap_idx < b.join_snapshot:
                    continue
                # Replay the change history up to this snapshot. Replaying rather
                # than storing per-snapshot state keeps memory flat and keeps the
                # snapshot a pure function of the broker plus the index.
                agency_idx, region, tier, name = b.agency_idx, b.region, b.tier, b.full_name
                is_active = True
                for c_snap, attr, val in b.changes:
                    if c_snap > snap_idx:
                        break
                    if attr == "agency_idx":
                        agency_idx = val
                    elif attr == "region":
                        region = val
                    elif attr == "tier":
                        tier = val
                    elif attr == "is_active":
                        is_active = val
                    elif attr == "full_name":
                        name = val
                agency = agencies[agency_idx]

                r = rng("dirrow", f"{b.nk}|{snap_idx}")
                email = b.email
                if r.random() < RATE["B02_directory_email_variant"]:
                    email = case_whitespace_noise(b.email, r)
                region_txt, tier_txt = region, tier
                if r.random() < RATE["B03_directory_text_variant"]:
                    region_txt = case_whitespace_noise(region, r)
                    tier_txt = case_whitespace_noise(tier, r)

                emit = 1
                if b.nk in dup_targets and snap_idx == min(len(snapshots) - 1, b.join_snapshot + 5):
                    emit = 2  # B01: the same broker listed twice in one snapshot
                for _ in range(emit):
                    line_no += 1
                    rows += 1
                    values = (
                        f"{snap:%Y-%m-%d}", b.nk, name, email, b.phone,
                        agency.code, agency.name, agency.tier, agency.province,
                        "Y" if agency.is_national else "N", agency.headcount_band,
                        region_txt, tier_txt, "Y" if is_active else "N",
                    )
                    buf.append(copy_line((*values, ingest_run, ingested_at, source_file,
                                          line_no, payload_hash(*values))))
            fh.write("".join(buf))
    return rows


def write_property_register(path: Path, props: list[Property], snapshots: list[date]) -> int:
    """Initial full load plus a change-only delta feed.

    Deliberately a DELTA feed while the broker directory is a FULL snapshot, so
    the SCD2 loader is proved against both source shapes.
    """
    rows = 0
    n_delta = max(1, round(len(props) * 0.1333))
    delta_ids = pick_exact([p.nk for p in props], "reclass", n_delta)
    with path.open("w", encoding="utf-8") as fh:
        ingest_run = f"ING-PRP-{snapshots[0]:%Y-%m}"
        ingested_at = ts_text(datetime.combine(snapshots[0], datetime.min.time()) + timedelta(hours=5))
        source_file = f"property_register_initial_{snapshots[0]:%Y_%m}.csv"
        line_no = 0
        buf = []
        for p in props:
            r = rng("prprow", p.nk)
            sector_txt = p.sector
            if sector_txt is not None and r.random() < RATE["P02_register_sector_variant"]:
                sector_txt = case_whitespace_noise(p.sector, r)
            line_no += 1
            rows += 1
            values = (
                p.nk, "INITIAL", f"{snapshots[0]:%Y-%m-%d}", p.name, p.suburb, p.metro,
                p.province, sector_txt, p.sub_sector, p.grade, str(p.gla_sqm), p.gla_band,
            )
            buf.append(copy_line((*values, ingest_run, ingested_at, source_file,
                                  line_no, payload_hash(*values))))
        fh.write("".join(buf))

        # Delta rows, one per reclassified property, spread across the window.
        per_month: dict[date, list[Property]] = {}
        for p in props:
            if p.nk not in delta_ids:
                continue
            r = rng("reclass_when", p.nk)
            snap = snapshots[6 + int(r.random() * (len(snapshots) - 12))]
            per_month.setdefault(snap, []).append(p)
        for snap in sorted(per_month):
            ingest_run = f"ING-PRP-{snap:%Y-%m}"
            ingested_at = ts_text(datetime.combine(snap, datetime.min.time()) + timedelta(hours=5))
            source_file = f"property_register_delta_{snap:%Y_%m}.csv"
            line_no = 0
            buf = []
            for p in per_month[snap]:
                r = rng("reclass_what", p.nk)
                sector, sub, grade, band = p.sector or "Industrial", p.sub_sector, p.grade, p.gla_band
                kind = r.random()
                if kind < 0.60:
                    # A rezoning is exactly why property needs Type 2: a 2021 deal
                    # must still report under the sector it was sold as.
                    others = [s for s in SECTOR_NAMES if s != sector]
                    sector = others[int(r.random() * len(others))]
                    sub = SECTORS[sector]["subs"][int(r.random() * len(SECTORS[sector]["subs"]))]
                elif kind < 0.75:
                    sub = SECTORS[sector]["subs"][int(r.random() * len(SECTORS[sector]["subs"]))]
                elif kind < 0.90:
                    grade = GRADES[int(r.random() * len(GRADES))]
                else:
                    band = GLA_BANDS[int(r.random() * len(GLA_BANDS))]
                line_no += 1
                rows += 1
                values = (
                    p.nk, "RECLASSIFY", f"{snap:%Y-%m-%d}", p.name, p.suburb, p.metro,
                    p.province, sector, sub, grade, str(p.gla_sqm), band,
                )
                buf.append(copy_line((*values, ingest_run, ingested_at, source_file,
                                      line_no, payload_hash(*values))))
            fh.write("".join(buf))
    return rows


# ---------------------------------------------------------------------------
# 5. Deals
# ---------------------------------------------------------------------------


@dataclass
class Deal:
    tmp_key: str
    nk: str
    broker_nk: str
    prop_idx: int
    value: float
    received: datetime
    channel: str
    off_market: bool
    is_parse_loss: bool = False


def allocate_monthly_counts(total: int, months: list[date]) -> list[int]:
    """Split the deal total across months by growth times seasonality.

    Largest-remainder apportionment, so the parts sum to the total exactly and
    the ledger has no rounding drift to explain.
    """
    weights = []
    for i, m in enumerate(months):
        weights.append(ANNUAL_GROWTH ** (i / 12.0) * MONTH_SEASONALITY[m.month])
    scale = total / sum(weights)
    raw = [w * scale for w in weights]
    base = [int(x) for x in raw]
    shortfall = total - sum(base)
    order = sorted(range(len(raw)), key=lambda i: (-(raw[i] - base[i]), i))
    for i in order[:shortfall]:
        base[i] += 1
    return base


def days_in_month(m: date) -> int:
    nxt = date(m.year + (m.month == 12), 1 if m.month == 12 else m.month + 1, 1)
    return (nxt - m).days


def day_weights(m: date) -> list[float]:
    """Weekdays carry the volume. Weekends get a small tail, because email intake
    does not stop on a Saturday, it just slows down."""
    out = []
    for d in range(1, days_in_month(m) + 1):
        wd = date(m.year, m.month, d).weekday()
        out.append(1.0 if wd < 5 else (0.10 if wd == 5 else 0.04))
    return out


def build_deals(
    months: list[date], counts: list[int], brokers: list[Broker], props: list[Property]
) -> list[Deal]:
    # Eligibility per month: a broker cannot receive a deal before they appear in
    # the directory or after they are deactivated. Building the cumulative weight
    # array per month keeps the power law intact inside the eligible set.
    n_snap = len(months)
    active_from = {b.nk: b.join_snapshot for b in brokers}
    deactivated_at: dict[str, int] = {}
    for b in brokers:
        for snap, attr, val in b.changes:
            if attr == "is_active" and val is False:
                deactivated_at[b.nk] = snap
                break

    prop_cum = _cum([p.weight for p in props])
    deals: list[Deal] = []

    for mi, month in enumerate(months):
        eligible = [
            b for b in brokers
            if active_from[b.nk] <= mi and deactivated_at.get(b.nk, n_snap) > mi
        ]
        broker_cum = _cum([b.productivity_rank ** -BROKER_POWER_LAW_BETA for b in eligible])
        dw = _cum(day_weights(month))
        n = counts[mi]
        for k in range(1, n + 1):
            tmp_key = f"{month:%Y%m}-{k:06d}"
            r = rng("deal", tmp_key)
            broker = eligible[weighted_index(broker_cum, r.random())]
            prop_idx = weighted_index(prop_cum, r.random())
            prop = props[prop_idx]
            sector = prop.sector or "Industrial"
            spec = SECTORS[sector]
            # Log-normal value. round() is applied to a Decimal-safe string later;
            # note that Postgres has no round(double precision, integer), so any
            # SQL doing this would need an explicit ::numeric cast.
            value = spec["median_zar"] * math.exp(spec["sigma"] * r.gauss(0.0, 1.0))
            day = 1 + weighted_index(dw, r.random())
            received = datetime(month.year, month.month, day) + timedelta(
                seconds=int(6 * 3600 + r.random() * 13 * 3600)
            )
            deals.append(
                Deal(
                    tmp_key=tmp_key,
                    nk="",
                    broker_nk=broker.nk,
                    prop_idx=prop_idx,
                    value=round(value, 2),
                    received=received,
                    channel=CHANNELS[weighted_index(CHANNEL_CUM, r.random())],
                    off_market=r.random() < 0.22,
                )
            )
    return deals


def write_deal_submissions(
    path: Path, deals: list[Deal], props: list[Property], brokers_by_nk: dict[str, Broker],
    scale: float = 1.0,
) -> dict[str, int]:
    """Write the submission feed and assign every deal its natural key.

    Duplicate resubmissions get their own deal reference and message id from the
    same monthly block as the original, in receipt order, so a duplicate is not
    identifiable from its key. That matters: it forces dedupe onto a content
    fingerprint, which is the whole point of the exercise.
    """
    stats = {k: 0 for k in (
        "rows", "duplicates", "value_null", "value_thousands", "value_unparseable",
        "date_unparseable", "email_variant", "unknown_broker", "property_name_only",
        "negative_value", "sector_hint_noise", "designated_losses",
    )}

    keys = [d.tmp_key for d in deals]
    dup_keys = pick_exact(keys, "dup_deal", round(len(deals) * RATE["D01_duplicate"]))
    # The designated total parse losses are drawn only from deals WITHOUT a
    # duplicate, so "total loss" really means every row of that deal is gone.
    loss_pool = [k for k in keys if k not in dup_keys]
    loss_keys = pick_exact(loss_pool, "parse_loss",
                           scaled_count("designated_parse_losses", scale))

    # Remaining unparseable budget goes onto the DUPLICATE row of a pair, never
    # the original, so every non-designated reject keeps a parseable twin and the
    # 249,000 invariant holds exactly.
    total_rows_est = len(deals) + len(dup_keys)
    budget = round(total_rows_est * (RATE["D04_value_unparseable"] + RATE["D05_date_unparseable"]))
    n_extra = max(0, budget - len(loss_keys))
    extra_keys = pick_exact(sorted(dup_keys), "extra_reject", min(n_extra, len(dup_keys)))

    # Bucket every row (original and duplicate) by the month it was received in,
    # because the raw feed arrives as one file per monthly ingest run and the
    # physical row order has to match ingest order for BRIN to be worth anything.
    buckets: dict[tuple[int, int], list[tuple[datetime, str, int, float, bool]]] = {}
    for idx, d in enumerate(deals):
        buckets.setdefault((d.received.year, d.received.month), []).append(
            (d.received, d.tmp_key, idx, d.value, False)
        )
        if d.tmp_key in dup_keys:
            r = rng("dup", d.tmp_key)
            # Same ISO week as the original, which is what the dedupe fingerprint
            # buckets on. A resubmission that crossed the week boundary would not
            # be caught by the agreed fingerprint at all.
            room = 6 - d.received.weekday()
            offset = int(r.random() * (room + 1)) if room > 0 else 0
            dts = d.received + timedelta(days=offset, seconds=int(r.random() * 20000))
            if dts > CUT_TS:
                dts = d.received + timedelta(seconds=1800)
            jitter = 1.0 if r.random() < 0.40 else 1.0 + (r.random() * 0.04 - 0.02)
            buckets.setdefault((dts.year, dts.month), []).append(
                (dts, d.tmp_key + "-D", idx, round(d.value * jitter, 2), True)
            )

    with path.open("w", encoding="utf-8") as fh:
        for (yy, mm) in sorted(buckets):
            rows = sorted(buckets[(yy, mm)], key=lambda t: (t[0], t[1]))
            ingest_run = f"ING-SUB-{yy:04d}-{mm:02d}"
            ingested_at = ts_text(datetime(yy, mm, days_in_month(date(yy, mm, 1)), 23, 30))
            source_file = f"deal_submissions_{yy:04d}_{mm:02d}.csv"
            buf = []
            for line_no, (ts, sort_key, deal_idx, value, is_dup) in enumerate(rows, start=1):
                deal = deals[deal_idx]
                deal_nk = f"DL-{yy:04d}{mm:02d}-{line_no:06d}"
                if not is_dup:
                    deal.nk = deal_nk
                stats["rows"] += 1
                if is_dup:
                    stats["duplicates"] += 1

                prop = props[deal.prop_idx]
                broker = brokers_by_nk[deal.broker_nk]
                r = rng("subrow", sort_key)

                # --- value defects, mutually exclusive, priority ordered -----
                value_txt: str | None = money_text(value)
                designated = (not is_dup) and deal.tmp_key in loss_keys
                unparseable = designated or (is_dup and deal.tmp_key in extra_keys)
                if unparseable:
                    if r.random() < 0.60:
                        value_txt = UNPARSEABLE_VALUES[int(r.random() * len(UNPARSEABLE_VALUES))]
                        stats["value_unparseable"] += 1
                        bad_date = False
                    else:
                        bad_date = True
                    if designated:
                        deal.is_parse_loss = True
                        stats["designated_losses"] += 1
                else:
                    bad_date = False
                    x = r.random()
                    if x < RATE["D02_value_null"]:
                        value_txt = None
                        stats["value_null"] += 1
                    elif x < RATE["D02_value_null"] + RATE["D03_value_thousands"]:
                        value_txt = money_text(value / 1000.0)
                        stats["value_thousands"] += 1
                    elif x < (RATE["D02_value_null"] + RATE["D03_value_thousands"]
                              + RATE["D09_negative_value"]):
                        value_txt = money_text(-value)
                        stats["negative_value"] += 1

                # --- received timestamp -------------------------------------
                if bad_date:
                    received_txt = UNPARSEABLE_DATES[int(r.random() * len(UNPARSEABLE_DATES))]
                    stats["date_unparseable"] += 1
                else:
                    received_txt = ts_text(ts)

                # --- broker identity ----------------------------------------
                broker_nk_txt, email_txt = broker.nk, broker.email
                if r.random() < RATE["D07_unknown_broker"]:
                    broker_nk_txt = f"BRK-9{int(r.random() * 89999) + 10000:05d}"
                    stats["unknown_broker"] += 1
                if r.random() < RATE["D06_email_variant"]:
                    email_txt = case_whitespace_noise(broker.email, r)
                    stats["email_variant"] += 1

                # --- property identity --------------------------------------
                prop_nk_txt: str | None = prop.nk
                prop_name_txt = prop.name
                if r.random() < RATE["D08_property_name_only"]:
                    prop_nk_txt = None
                    prop_name_txt = prop.name + " (Pty) Ltd"
                    stats["property_name_only"] += 1

                sector_txt = prop.sector or ""
                if prop.sector and r.random() < RATE["D10_sector_hint_noise"]:
                    sector_txt = case_whitespace_noise(prop.sector, r)
                    stats["sector_hint_noise"] += 1

                values = (
                    f"MSG-{yy:04d}{mm:02d}-{line_no:06d}", deal_nk, broker_nk_txt,
                    broker.full_name, email_txt, prop_nk_txt, prop_name_txt, sector_txt,
                    value_txt, "ZAR", received_txt, deal.channel,
                    "Y" if deal.off_market else "N",
                    NOTE_SNIPPETS[int(r.random() * len(NOTE_SNIPPETS))],
                )
                buf.append(copy_line((*values, ingest_run, ingested_at, source_file,
                                      line_no, payload_hash(*values))))
            fh.write("".join(buf))
    return stats


# ---------------------------------------------------------------------------
# 6. Stage events
# ---------------------------------------------------------------------------


def business_moment(base: datetime, prev: datetime, r: random.Random) -> datetime:
    """Land an event inside working hours on a working day, strictly after prev."""
    d = base.date()
    while d.weekday() >= 5:
        d += timedelta(days=1)
    ts = datetime.combine(d, datetime.min.time()) + timedelta(
        seconds=int(7 * 3600 + r.random() * 10.5 * 3600)
    )
    if ts <= prev:
        ts = prev + timedelta(seconds=int(3600 + r.random() * 6 * 3600))
    return ts


def walk_funnel(deal: Deal) -> list[tuple[int, datetime, float | None]]:
    """One deal's stage transitions: (to_stage, event_ts, revised_value_or_None).

    The stall threshold is a per-deal Normal(90, 15) draw with a floor at 45.
    A hard 90 day cutoff would put a spike at exactly 90.00 days into the p90 of
    five different stages, which is the single most obvious tell that a dataset
    was generated rather than observed.
    """
    r = rng("funnel", deal.nk)
    stall_threshold = max(45.0, r.gauss(90.0, 15.0))
    events: list[tuple[int, datetime, float | None]] = []
    ts = deal.received
    events.append((1, ts, None))
    stage = 1
    churn_used = False
    current_value = deal.value

    while stage < 8:
        stall_p, lost_p, median = STAGE_P[stage]
        x = r.random()
        if x < stall_p:
            events.append((10, business_moment(ts + timedelta(days=stall_threshold), ts, r), None))
            break
        if x < stall_p + lost_p:
            d = lognormal(r, median * LOST_DWELL_MULTIPLIER)
            events.append((9, business_moment(ts + timedelta(days=d), ts, r), None))
            break
        d = lognormal(r, median)
        ts = business_moment(ts + timedelta(days=d), ts, r)
        stage += 1
        rev = None
        if r.random() < VALUE_REVISION_P:
            current_value = round(current_value * (1.0 + (r.random() * 0.20 - 0.10)), 2)
            rev = current_value
        events.append((stage, ts, rev))

        # One legal single-step backward move, then forward again: re-underwriting
        # after due diligence findings is normal, and it gives reopen_count and
        # is_forward_move something real to measure. Never more than one step
        # back, because that would be a physically impossible transition.
        if not churn_used and stage >= 4 and r.random() < CHURN_P:
            churn_used = True
            ts = business_moment(ts + timedelta(days=lognormal(r, 6.0)), ts, r)
            events.append((stage - 1, ts, None))
            ts = business_moment(ts + timedelta(days=lognormal(r, 10.0)), ts, r)
            events.append((stage, ts, None))

    return [e for e in events if e[1] <= CUT_TS]


def write_stage_events(
    out_dir: Path, deals: list[Deal], scale: float = 1.0
) -> tuple[list[Path], dict[str, int]]:
    """Write one COPY file per event month, so raw rows land in ingest order.

    Physical order matters: BRIN on ingested_at is only worth anything on a table
    whose rows are physically ordered by that column, and monthly ingest runs are
    what make that true here.
    """
    stats = {k: 0 for k in (
        "accepted", "redelivered", "impossible", "unknown_stage", "orphan",
        "before_receipt", "out_of_range", "rows",
    )}
    handles: dict[tuple[int, int], object] = {}
    line_no: dict[tuple[int, int], int] = {}
    buffers: dict[tuple[int, int], list[str]] = {}

    live = [d.nk for d in deals if not d.is_parse_loss]
    impossible_keys = pick_exact(
        live, "impossible", min(len(live), scaled_count("E02_impossible_transitions", scale))
    )
    unknown_stage_keys = pick_exact(
        live, "unknown_stage", min(len(live), scaled_count("E03_unknown_stage_codes", scale))
    )

    def emit(values: tuple, key: tuple[int, int]) -> None:
        if key not in handles:
            handles[key] = (out_dir / f"stage_events_{key[0]:04d}_{key[1]:02d}.tsv").open(
                "w", encoding="utf-8"
            )
            line_no[key] = 0
            buffers[key] = []
        line_no[key] += 1
        yy, mm = key
        ingest_run = f"ING-EVT-{yy:04d}-{mm:02d}"
        ingested_at = ts_text(datetime(yy, mm, days_in_month(date(yy, mm, 1)), 23, 45))
        source_file = f"pipeline_events_{yy:04d}_{mm:02d}.csv"
        buffers[key].append(copy_line((*values, ingest_run, ingested_at, source_file,
                                       line_no[key], payload_hash(*values))))
        stats["rows"] += 1
        if len(buffers[key]) >= 20000:
            handles[key].write("".join(buffers[key]))
            buffers[key].clear()

    for deal in deals:
        if deal.is_parse_loss:
            # No submission survived staging, so the pipeline system never created
            # this deal and it cannot have emitted events. Skipping them here is
            # what stops the seeded parse losses from becoming orphan facts and
            # tripping a blocking DQ gate on the demo data.
            continue

        events = walk_funnel(deal)
        r = rng("evtnoise", deal.nk)
        n_events = len(events)
        for seq, (stage, ts, rev) in enumerate(events, start=1):
            event_nk = "EVT-" + md5(f"{deal.nk}|{seq}".encode()).hexdigest()[:12].upper()
            ingest_key = (ts.year, ts.month)
            event_ts = ts
            note = None

            # E05: a capture error puts the event before the deal was received.
            if seq > 1 and r.random() < RATE["E05_event_before_receipt"]:
                event_ts = deal.received - timedelta(days=1 + int(r.random() * 20))
                stats["before_receipt"] += 1
                note = "captured retrospectively"
            # E06: a typed year lands the event outside the fact partition range,
            # which is exactly what the DEFAULT partition is there to absorb.
            elif r.random() < RATE["E06_event_out_of_range"]:
                event_ts = shift_year(event_ts, 2031 if r.random() < 0.7 else 2019)
                stats["out_of_range"] += 1
                note = "date suspect"

            stats["accepted"] += 1
            values = (
                event_nk, deal.nk, STAGE_CODES[stage], ts_text(event_ts), deal.broker_nk,
                money_text(rev) if rev is not None else None, note,
            )
            emit(values, ingest_key)

            # E01: the source system redelivers a payload verbatim. Same event id,
            # same content, new line in the same file.
            if r.random() < RATE["E01_event_redelivered"]:
                stats["redelivered"] += 1
                emit(values, ingest_key)

            # E04: an event for a deal reference that was never submitted.
            if r.random() < RATE["E04_orphan_event"]:
                stats["orphan"] += 1
                ghost = f"DL-{ts.year:04d}{ts.month:02d}-9{int(r.random() * 89999) + 10000:05d}"
                gvals = (
                    "EVT-" + md5(f"{ghost}|{seq}".encode()).hexdigest()[:12].upper(),
                    ghost, STAGE_CODES[stage], ts_text(ts), deal.broker_nk, None,
                    "unmatched deal reference",
                )
                emit(gvals, ingest_key)

        if not events:
            continue
        last_stage, last_ts, _ = events[-1]

        # E02: physically impossible transitions. Either out of a terminal stage
        # or backwards three or more stages. The intermediate layer must
        # quarantine these; they must never reach the fact.
        if deal.nk in impossible_keys:
            bad_ts = business_moment(last_ts + timedelta(days=lognormal(r, 12.0)), last_ts, r)
            if bad_ts <= CUT_TS:
                stats["impossible"] += 1
                target = 4 if last_stage >= 8 else max(1, last_stage - 3)
                vals = (
                    "EVT-" + md5(f"{deal.nk}|imp".encode()).hexdigest()[:12].upper(),
                    deal.nk, STAGE_CODES[target], ts_text(bad_ts), deal.broker_nk, None,
                    "sequence anomaly",
                )
                emit(vals, (bad_ts.year, bad_ts.month))

        # E03: a stage code the warehouse has never heard of.
        if deal.nk in unknown_stage_keys:
            odd_ts = business_moment(last_ts + timedelta(days=lognormal(r, 8.0)), last_ts, r)
            if odd_ts <= CUT_TS:
                stats["unknown_stage"] += 1
                vals = (
                    "EVT-" + md5(f"{deal.nk}|unk".encode()).hexdigest()[:12].upper(),
                    deal.nk,
                    UNKNOWN_STAGE_CODES[int(r.random() * len(UNKNOWN_STAGE_CODES))],
                    ts_text(odd_ts), deal.broker_nk, None, "unmapped stage code",
                )
                emit(vals, (odd_ts.year, odd_ts.month))

    paths = []
    for key in sorted(handles):
        handles[key].write("".join(buffers[key]))
        handles[key].close()
        paths.append(out_dir / f"stage_events_{key[0]:04d}_{key[1]:02d}.tsv")
    return paths, stats


# ---------------------------------------------------------------------------
# 7. DDL and loading
# ---------------------------------------------------------------------------

RAW_COLUMNS = {
    "deal_submission": [
        "source_message_nk", "deal_nk", "broker_nk", "broker_name", "broker_email",
        "property_nk", "property_name", "sector_hint", "deal_value_zar", "currency_code",
        "received_ts", "source_channel", "is_off_market", "submission_notes",
        "source_batch_id", "ingested_at", "source_file", "raw_line_no", "payload_hash",
    ],
    "stage_event": [
        "source_event_nk", "deal_nk", "stage_code", "event_ts", "actor_broker_nk",
        "stage_value_zar", "event_notes",
        "source_batch_id", "ingested_at", "source_file", "raw_line_no", "payload_hash",
    ],
    "broker_directory": [
        "snapshot_date", "broker_nk", "full_name", "broker_email", "broker_phone",
        "agency_code", "agency_name", "agency_tier", "agency_head_office_province",
        "agency_is_national", "agency_headcount_band", "region", "broker_tier", "is_active",
        "source_batch_id", "ingested_at", "source_file", "raw_line_no", "payload_hash",
    ],
    "property_register": [
        "property_nk", "register_event_type", "effective_date", "property_name", "suburb",
        "metro", "province", "sector", "sub_sector", "building_grade", "gla_sqm",
        "gla_sqm_band",
        "source_batch_id", "ingested_at", "source_file", "raw_line_no", "payload_hash",
    ],
}

GRAIN = {
    "deal_submission": "One row per inbound broker deal submission exactly as received, "
                       "including duplicate resubmissions and unparseable values.",
    "stage_event": "One row per deal stage transition payload as received from the pipeline "
                   "system, including redeliveries, out of order and impossible events.",
    "broker_directory": "One row per broker per monthly full snapshot of the broker directory.",
    "property_register": "One row per property register event: an initial full load plus a "
                         "change only delta feed thereafter.",
}


def raw_ddl(schema: str) -> str:
    """Landing zone DDL. Every SOURCE column is text and nothing is constrained,
    because raw is the audit trail: if a transform is wrong you must be able to
    replay from here without re-fetching. Only the warehouse's own ingest
    metadata is typed and NOT NULL.
    """
    parts = [f"CREATE SCHEMA IF NOT EXISTS {schema};"]
    for table, cols in RAW_COLUMNS.items():
        body = []
        for c in cols:
            if c == "ingested_at":
                body.append("    ingested_at  timestamptz NOT NULL")
            elif c == "raw_line_no":
                body.append("    raw_line_no  bigint      NOT NULL")
            elif c in ("source_batch_id", "source_file", "payload_hash"):
                body.append(f"    {c:12s} text        NOT NULL")
            else:
                body.append(f"    {c:12s} text")
        parts.append(
            f"CREATE TABLE IF NOT EXISTS {schema}.{table} (\n"
            + ",\n".join(body)
            + "\n);"
        )
        parts.append(
            f"COMMENT ON TABLE {schema}.{table} IS "
            + sql_literal(GRAIN[table])
            + ";"
        )
    return "\n".join(parts)


def raw_index_ddl(schema: str) -> str:
    """BRIN on ingested_at, and BRIN belongs HERE rather than on the partitioned
    fact. These landing tables are one large heap physically ordered by ingest
    time, which is the only situation where a block range summary can exclude
    anything. Default pages_per_range: there is no measured reason to override it.

    Index names match sql/01_staging.sql exactly. They have to: 01 owns the
    landing zone when the generator is pointed at the real `raw` schema, and a
    second BRIN under a different name would be a duplicate index that nobody
    asked for and that every INSERT would then have to maintain.
    """
    out = []
    for table in RAW_COLUMNS:
        out.append(
            f"CREATE INDEX IF NOT EXISTS ix_raw_{table}_brin "
            f"ON {schema}.{table} USING brin (ingested_at);"
        )
    return "\n".join(out)


def sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


class Psql:
    """Thin wrapper over psql inside the container. Password comes from the
    environment and is never echoed, logged or written to disk."""

    def __init__(self, container: str, user: str, db: str, password: str) -> None:
        self.container = container
        self.user = user
        self.db = db
        self.env = dict(os.environ)
        self.env["PGPASSWORD"] = password

    def _base(self) -> list[str]:
        return [
            "docker", "exec", "-i", "-e", "PGPASSWORD", self.container,
            "psql", "-U", self.user, "-d", self.db, "-v", "ON_ERROR_STOP=1", "-q",
        ]

    def run(self, sql: str) -> str:
        # -c carries the statement, so stdin is closed rather than fed: passing the
        # SQL twice would leave psql holding an unread pipe.
        proc = subprocess.run(
            self._base() + ["-At", "-F", "|", "-c", sql],
            input=b"", env=self.env, capture_output=True,
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode().strip())
        return proc.stdout.decode()

    def script(self, sql: str) -> None:
        proc = subprocess.run(
            self._base() + ["-f", "-"], input=sql.encode(), env=self.env, capture_output=True
        )
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.decode().strip())

    def copy_files(self, schema: str, table: str, files: Sequence[Path]) -> None:
        cols = ", ".join(RAW_COLUMNS[table])
        cmd = self._base() + ["-c", f"COPY {schema}.{table} ({cols}) FROM STDIN"]
        proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, env=self.env)
        assert proc.stdin is not None
        try:
            for f in files:
                with f.open("rb") as fh:
                    shutil.copyfileobj(fh, proc.stdin, length=1 << 20)
        finally:
            proc.stdin.close()
        out, err = proc.communicate()
        if proc.returncode != 0:
            raise RuntimeError(err.decode().strip())


def read_env_password(env_path: Path) -> str:
    if not env_path.exists():
        raise SystemExit(f"cannot find {env_path}, expected POSTGRES_PASSWORD there")
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line.startswith("POSTGRES_PASSWORD="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    raise SystemExit("POSTGRES_PASSWORD not found in .env")


# ---------------------------------------------------------------------------
# 8. Reporting. Every number comes from the database, none from this file.
# ---------------------------------------------------------------------------

def report_sql(schema: str) -> list[tuple[str, str]]:
    """Named probes. Values are cast defensively because raw is all text: a
    report that crashes on the dirty rows it was built to describe is useless.
    """
    num = (
        "CASE WHEN deal_value_zar ~ '^-?[0-9]+(\\.[0-9]+)?$' "
        "THEN deal_value_zar::numeric END"
    )
    ts = (
        "CASE WHEN received_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} ' "
        "THEN received_ts::timestamptz END"
    )
    ets = (
        "CASE WHEN event_ts ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2} ' "
        "THEN event_ts::timestamptz END"
    )
    return [
        ("row_counts", f"""
            SELECT 'raw.deal_submission', count(*) FROM {schema}.deal_submission
            UNION ALL SELECT 'raw.stage_event', count(*) FROM {schema}.stage_event
            UNION ALL SELECT 'raw.broker_directory', count(*) FROM {schema}.broker_directory
            UNION ALL SELECT 'raw.property_register', count(*) FROM {schema}.property_register
        """),
        ("submission_ledger", f"""
            -- Raw is all text, so "missing" and "unparseable" are different things
            -- and the ledger has to keep them apart: a NULL value still stages, an
            -- unparseable one is a hard reject.
            WITH parsed AS (
                SELECT deal_nk, broker_nk, property_nk, deal_value_zar AS v_txt,
                       {num} AS v, {ts} AS rts
                FROM {schema}.deal_submission
            )
            SELECT 'rows total', count(*) FROM parsed
            UNION ALL SELECT 'hard reject: value unparseable text',
                   count(*) FROM parsed WHERE v_txt IS NOT NULL AND v IS NULL
            UNION ALL SELECT 'hard reject: received_ts unparseable',
                   count(*) FROM parsed WHERE rts IS NULL
            UNION ALL SELECT 'hard rejects total',
                   count(*) FROM parsed WHERE rts IS NULL OR (v_txt IS NOT NULL AND v IS NULL)
            UNION ALL SELECT 'soft: value absent (NULL)',
                   count(*) FROM parsed WHERE v_txt IS NULL
            UNION ALL SELECT 'soft: negative value',
                   count(*) FROM parsed WHERE v < 0
            UNION ALL SELECT 'stageable rows',
                   count(*) FROM parsed WHERE rts IS NOT NULL
                     AND NOT (v_txt IS NOT NULL AND v IS NULL)
            UNION ALL SELECT 'distinct broker refs', count(DISTINCT broker_nk) FROM parsed
            UNION ALL SELECT 'broker refs unknown to the directory',
                   count(*) FROM parsed p WHERE NOT EXISTS (
                       SELECT 1 FROM {schema}.broker_directory b
                       WHERE b.broker_nk = p.broker_nk)
            UNION ALL SELECT 'rows with no property ref',
                   count(*) FROM parsed WHERE property_nk IS NULL
        """),
        ("dedupe_candidates", f"""
            -- Scores candidate value buckets for the intermediate-layer dedupe
            -- fingerprint against the duplicates this generator actually seeded.
            -- It exists because the value bucket agreed in the design,
            -- round(value / 100000), is ABSOLUTE while the seeded duplicate defect
            -- is PROPORTIONAL (value jittered up to 2%), and 2% of a R28.9m deal is
            -- 5.8 buckets wide. Columns are: pairs caught, pairs missed, and false
            -- merges, where a false merge is a group holding two genuinely
            -- different deals that the rule would wrongly collapse into one.
            WITH parsed AS (
                SELECT broker_nk, property_nk, {num} AS v, {ts} AS rts
                FROM {schema}.deal_submission
            ), elig AS (
                -- v > 0 is not tidiness: the seeded negative-value defect would make
                -- log(v) raise, and property_nk IS NULL rows cannot be fingerprinted
                -- at all until staging has conformed them by name.
                SELECT broker_nk, property_nk, v, date_trunc('week', rts) AS wk
                FROM parsed
                WHERE v IS NOT NULL AND v > 0 AND rts IS NOT NULL AND property_nk IS NOT NULL
            ), grp AS (
                SELECT count(*) AS n, max(v) / min(v) AS ratio,
                       count(DISTINCT round(v / 100000))         AS b_abs,
                       count(DISTINCT round(log(v) / log(1.05))) AS b_rel05,
                       count(DISTINCT round(log(v) / log(1.10))) AS b_rel10,
                       count(DISTINCT round(log(v) / log(1.25))) AS b_rel25
                FROM elig
                GROUP BY broker_nk, property_nk, wk
            ), dup AS (
                -- A pair inside one (broker, property, week) whose two values differ
                -- by no more than the 2% jitter ceiling is a resubmission, which is
                -- what makes the catch rate below measurable rather than assumed.
                SELECT * FROM grp WHERE n = 2 AND ratio <= 1.02
            ), dis AS (
                SELECT * FROM grp WHERE n > 1 AND ratio > 1.02
            )
            SELECT 'groups on (broker, property, week)' AS rule,
                   (SELECT count(*) FROM grp)::text, ''::text, ''::text
            UNION ALL SELECT 'true duplicate pairs visible in the eligible set',
                   (SELECT count(*) FROM dup)::text, ''::text, ''::text
            UNION ALL SELECT 'caught / missed / false-merge BY RULE:', '', '', ''
            UNION ALL SELECT '  round(v/100000)  AGREED SPEC',
                   (SELECT count(*) FROM dup WHERE b_abs = 1)::text,
                   (SELECT count(*) FROM dup WHERE b_abs > 1)::text,
                   (SELECT count(*) FROM dis WHERE b_abs = 1)::text
            UNION ALL SELECT '  round(log(v)/log(1.05))  5 pct bands',
                   (SELECT count(*) FROM dup WHERE b_rel05 = 1)::text,
                   (SELECT count(*) FROM dup WHERE b_rel05 > 1)::text,
                   (SELECT count(*) FROM dis WHERE b_rel05 = 1)::text
            UNION ALL SELECT '  round(log(v)/log(1.10))  10 pct bands',
                   (SELECT count(*) FROM dup WHERE b_rel10 = 1)::text,
                   (SELECT count(*) FROM dup WHERE b_rel10 > 1)::text,
                   (SELECT count(*) FROM dis WHERE b_rel10 = 1)::text
            UNION ALL SELECT '  round(log(v)/log(1.25))  25 pct bands',
                   (SELECT count(*) FROM dup WHERE b_rel25 = 1)::text,
                   (SELECT count(*) FROM dup WHERE b_rel25 > 1)::text,
                   (SELECT count(*) FROM dis WHERE b_rel25 = 1)::text
            UNION ALL SELECT '  no value term at all',
                   (SELECT count(*) FROM dup)::text, '0',
                   (SELECT count(*) FROM dis)::text
            UNION ALL SELECT '  no value term, 3 pct tolerance  RECOMMENDED',
                   (SELECT count(*) FROM dup)::text, '0',
                   (SELECT count(*) FROM grp
                    WHERE n > 1 AND ratio > 1.02 AND ratio <= 1.03)::text
        """),
        ("broker_concentration", f"""
            WITH per_broker AS (
                SELECT broker_nk, count(*) AS deals
                FROM {schema}.deal_submission
                WHERE broker_nk LIKE 'BRK-0%'
                GROUP BY 1
            ), d AS (
                SELECT deals, ntile(10) OVER (ORDER BY deals DESC) AS decile,
                       sum(deals) OVER () AS total
                FROM per_broker
            )
            SELECT 'decile ' || decile,
                   round(100.0 * sum(deals) / max(total), 2)
            FROM d GROUP BY decile ORDER BY decile
        """),
        ("sector_value", f"""
            -- NOTE the explicit ::numeric casts. PostgreSQL has no
            -- round(double precision, integer), and percentile_cont returns double
            -- precision, so this probe does not run without them.
            SELECT s.sector,
                   count(*),
                   round((percentile_cont(0.5) WITHIN GROUP (ORDER BY s.v))::numeric / 1e6, 1)
                       AS median_zar_m,
                   round(avg(s.v)::numeric / 1e6, 1) AS mean_zar_m,
                   round((percentile_cont(0.99) WITHIN GROUP (ORDER BY s.v))::numeric / 1e6, 1)
                       AS p99_zar_m
            FROM (
                -- initcap plus btrim is not enough on its own: the seeded case and
                -- whitespace defect also injects DOUBLE spaces inside the word, so
                -- without collapsing internal runs of whitespace "Mixed  Use" reports
                -- as a ninth sector. Staging has to do exactly this, which is the
                -- point of doing it here too.
                SELECT btrim(regexp_replace(initcap(pr.sector), '\\s+', ' ', 'g')) AS sector,
                       {num} AS v
                FROM {schema}.deal_submission ds
                JOIN {schema}.property_register pr
                  ON pr.property_nk = ds.property_nk AND pr.register_event_type = 'INITIAL'
                WHERE {num} IS NOT NULL AND {num} > 0 AND pr.sector IS NOT NULL
            ) s
            GROUP BY 1 ORDER BY 3 DESC
        """),
        ("seasonality", f"""
            WITH m AS (
                SELECT date_trunc('month', {ts}) AS mth, count(*) AS n
                FROM {schema}.deal_submission
                WHERE {ts} IS NOT NULL
                GROUP BY 1
            )
            SELECT to_char(mth, 'MM') AS cal_month,
                   round(avg(n)) AS avg_deals,
                   round(avg(n) / (SELECT avg(n) FROM m), 3) AS vs_overall_avg
            FROM m GROUP BY 1 ORDER BY 1
        """),
        ("growth_jan_on_jan", f"""
            SELECT to_char(date_trunc('month', {ts}), 'YYYY-MM'), count(*)
            FROM {schema}.deal_submission
            WHERE {ts} IS NOT NULL AND extract(month FROM {ts}) = 1
            GROUP BY 1 ORDER BY 1
        """),
        ("channel_mix", f"""
            SELECT source_channel, count(*),
                   round(100.0 * count(*) / sum(count(*)) OVER (), 2)
            FROM {schema}.deal_submission GROUP BY 1 ORDER BY 2 DESC
        """),
        ("funnel_reach", f"""
            WITH first_hit AS (
                SELECT DISTINCT deal_nk, stage_code
                FROM {schema}.stage_event
                WHERE stage_code IN ('RECEIVED','TRIAGED','QUALIFIED','UNDERWRITING',
                                     'OFFER','DUE_DILIGENCE','LEGAL','CLOSED_WON','LOST','STALLED')
            ), base AS (
                SELECT count(*) AS n FROM first_hit WHERE stage_code = 'RECEIVED'
            )
            SELECT stage_code, count(*),
                   round(100.0 * count(*) / (SELECT n FROM base), 2) AS pct_of_received
            FROM first_hit GROUP BY 1
            ORDER BY 2 DESC
        """),
        ("terminal_split", f"""
            WITH last_stage AS (
                SELECT DISTINCT ON (deal_nk) deal_nk, stage_code
                FROM {schema}.stage_event
                WHERE stage_code IN ('RECEIVED','TRIAGED','QUALIFIED','UNDERWRITING','OFFER',
                                     'DUE_DILIGENCE','LEGAL','CLOSED_WON','LOST','STALLED')
                  AND {ets} IS NOT NULL AND {ets} < '2027-01-01'
                ORDER BY deal_nk, {ets} DESC, raw_line_no DESC
            )
            SELECT CASE WHEN stage_code IN ('CLOSED_WON','LOST','STALLED')
                        THEN stage_code ELSE 'STILL OPEN (' || stage_code || ')' END AS outcome,
                   count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 2)
            FROM last_stage GROUP BY 1 ORDER BY 2 DESC
        """),
        ("dwell_percentiles", f"""
            -- Proves there is no spike at exactly 90.00 days, which is what a hard
            -- coded stall cutoff would produce and what gives a generator away.
            WITH seq AS (
                SELECT deal_nk, stage_code, {ets} AS ets,
                       lag({ets}) OVER (PARTITION BY deal_nk ORDER BY {ets}, raw_line_no) AS prev_ts
                FROM {schema}.stage_event
                WHERE {ets} IS NOT NULL AND {ets} < '2027-01-01'
                  AND stage_code IN ('RECEIVED','TRIAGED','QUALIFIED','UNDERWRITING','OFFER',
                                     'DUE_DILIGENCE','LEGAL','CLOSED_WON','LOST','STALLED')
            )
            SELECT stage_code, count(*),
                   round((percentile_cont(0.5) WITHIN GROUP (
                       ORDER BY extract(epoch FROM ets - prev_ts) / 86400))::numeric, 2) AS p50_days,
                   round((percentile_cont(0.9) WITHIN GROUP (
                       ORDER BY extract(epoch FROM ets - prev_ts) / 86400))::numeric, 2) AS p90_days
            FROM seq WHERE prev_ts IS NOT NULL
            GROUP BY 1 ORDER BY 3
        """),
        ("event_defects", f"""
            SELECT 'redelivered verbatim (extra rows)',
                   count(*) - count(DISTINCT source_event_nk) FROM {schema}.stage_event
            UNION ALL SELECT 'unknown stage code', count(*) FROM {schema}.stage_event
                   WHERE stage_code NOT IN ('RECEIVED','TRIAGED','QUALIFIED','UNDERWRITING',
                        'OFFER','DUE_DILIGENCE','LEGAL','CLOSED_WON','LOST','STALLED')
            UNION ALL SELECT 'event date outside 2020-01..2026-12', count(*)
                   FROM {schema}.stage_event
                   WHERE {ets} IS NOT NULL
                     AND ({ets} < '2020-01-01' OR {ets} >= '2027-01-01')
            UNION ALL SELECT 'orphan deal reference', count(*) FROM {schema}.stage_event e
                   WHERE NOT EXISTS (SELECT 1 FROM {schema}.deal_submission d
                                     WHERE d.deal_nk = e.deal_nk)
            UNION ALL SELECT 'event before its deal received_ts', count(*)
                   FROM {schema}.stage_event e
                   JOIN {schema}.deal_submission d ON d.deal_nk = e.deal_nk
                   WHERE {ets} IS NOT NULL AND {ts} IS NOT NULL AND {ets} < {ts}
        """),
        ("directory_shape", f"""
            SELECT 'snapshots', count(DISTINCT snapshot_date) FROM {schema}.broker_directory
            UNION ALL SELECT 'distinct brokers', count(DISTINCT broker_nk)
                   FROM {schema}.broker_directory
            UNION ALL SELECT 'brokers absent from the first snapshot',
                   (SELECT count(DISTINCT broker_nk) FROM {schema}.broker_directory)
                 - (SELECT count(*) FROM {schema}.broker_directory
                    WHERE snapshot_date = (SELECT min(snapshot_date)
                                           FROM {schema}.broker_directory))
            UNION ALL SELECT 'duplicate broker rows within one snapshot',
                   coalesce((SELECT sum(n - 1) FROM (
                        SELECT count(*) AS n FROM {schema}.broker_directory
                        GROUP BY snapshot_date, broker_nk HAVING count(*) > 1) x), 0)
            UNION ALL SELECT 'rows needing text conforming (case or whitespace)',
                   count(*) FROM {schema}.broker_directory
                   WHERE broker_email <> btrim(lower(broker_email))
                      OR region <> btrim(region) OR broker_tier <> btrim(broker_tier)
        """),
        ("property_shape", f"""
            SELECT 'initial rows', count(*) FROM {schema}.property_register
                   WHERE register_event_type = 'INITIAL'
            UNION ALL SELECT 'delta rows', count(*) FROM {schema}.property_register
                   WHERE register_event_type = 'RECLASSIFY'
            UNION ALL SELECT 'null sector rows', count(*) FROM {schema}.property_register
                   WHERE sector IS NULL
            UNION ALL SELECT 'properties never submitted', count(*)
                   FROM (SELECT property_nk FROM {schema}.property_register
                         WHERE register_event_type = 'INITIAL') p
                   WHERE NOT EXISTS (SELECT 1 FROM {schema}.deal_submission d
                                     WHERE d.property_nk = p.property_nk)
        """),
        ("fingerprint", f"""
            -- Dataset fingerprint. A rebuild that changes this changed the data.
            SELECT 'deal_submission', md5(string_agg(payload_hash, '' ORDER BY payload_hash))
            FROM {schema}.deal_submission
            UNION ALL
            SELECT 'stage_event', md5(string_agg(payload_hash, '' ORDER BY payload_hash))
            FROM {schema}.stage_event
        """),
    ]


def print_report(pg: Psql, schema: str) -> dict[str, list[list[str]]]:
    collected: dict[str, list[list[str]]] = {}
    for name, sql in report_sql(schema):
        out = pg.run(sql)
        rows = [line.split("|") for line in out.strip().splitlines() if line]
        collected[name] = rows
        print(f"\n--- {name} " + "-" * max(0, 60 - len(name)))
        for row in rows:
            print("    " + "  ".join(f"{c:>16s}" if i else f"{c:<44s}"
                                     for i, c in enumerate(row)))
    return collected


# ---------------------------------------------------------------------------
# 9. Self checks
# ---------------------------------------------------------------------------


def self_check(pg: Psql | None) -> None:
    """Cheap proofs that the determinism contract holds. Run before trusting a
    dataset: a generator that is not reproducible is not evidence of anything."""
    print("determinism self check")
    a = [u("value", f"DL-2024-{i}") for i in range(5)]
    b = [u("value", f"DL-2024-{i}") for i in range(5)]
    assert a == b, "u() is not stable within a process"
    print(f"    u() stable within process: True  (first draw {a[0]:.12f})")

    r1 = rng("funnel", "DL-202403-000117")
    r2 = rng("funnel", "DL-202403-000117")
    assert [r1.random() for _ in range(8)] == [r2.random() for _ in range(8)]
    print("    rng() stream reproducible for the same key: True")

    keys = [f"K{i}" for i in range(5000)]
    s1 = pick_exact(keys, "x", 137)
    s2 = pick_exact(list(reversed(keys)), "x", 137)
    assert s1 == s2 and len(s1) == 137
    print("    pick_exact() exact and order independent: True (137 of 5000)")

    if pg is not None:
        # Parity with the SQL side. If these ever diverge, a value generated in
        # Python and a value generated in SQL would silently disagree.
        pg.script("""
            CREATE SCHEMA IF NOT EXISTS util;
            CREATE OR REPLACE FUNCTION util.fn_u(p_seed text, p_key text)
            RETURNS double precision LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
                SELECT ('x' || substr(md5(p_seed || p_key), 1, 8))::bit(32)::bigint
                       / 4294967296.0;
            $$;
        """)
        key = "DL-202403-000117"
        sql_val = float(pg.run(
            f"SELECT util.fn_u('{DEALFLOW_SEED}|value|', '{key}')"
        ).strip())
        py_val = u("value", key)
        assert abs(sql_val - py_val) < 1e-12, (sql_val, py_val)
        print(f"    util.fn_u parity with Python u(): True  ({py_val:.12f})")


# ---------------------------------------------------------------------------
# 10. Entry point
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="DealFlow synthetic raw data generator")
    ap.add_argument("--schema", default="raw", help="target schema (default raw)")
    ap.add_argument("--container", default="dealflow-db")
    ap.add_argument("--user", default="dealflow")
    ap.add_argument("--db", default="dealflow")
    ap.add_argument("--env-file", default=str(Path(__file__).with_name(".env")))
    ap.add_argument("--out-dir", default=None, help="where COPY files are staged")
    ap.add_argument("--keep-files", action="store_true")
    ap.add_argument("--scale", type=float, default=1.0,
                    help="scale deals and properties (shape metrics only meaningful at 1.0)")
    ap.add_argument("--deals", type=int, default=250_000)
    ap.add_argument("--properties", type=int, default=12_000)
    ap.add_argument("--brokers", type=int, default=800)
    ap.add_argument("--agencies", type=int, default=60)
    ap.add_argument("--truncate", action="store_true", help="empty the target tables first")
    ap.add_argument("--append", action="store_true",
                    help="deliberately load a second dataset into a non-empty schema")
    ap.add_argument("--no-load", action="store_true", help="write files only")
    ap.add_argument("--report-only", action="store_true", help="re-print the ledger and exit")
    ap.add_argument("--self-check", action="store_true", help="determinism checks and exit")
    ap.add_argument("--ledger-out", default=None, help="write the ledger as JSON here")
    args = ap.parse_args(argv)

    password = read_env_password(Path(args.env_file))
    pg = Psql(args.container, args.user, args.db, password)

    if args.self_check:
        self_check(pg)
        return 0

    if args.report_only:
        print_report(pg, args.schema)
        return 0

    n_deals = max(100, int(args.deals * args.scale))
    n_props = max(20, int(args.properties * args.scale))
    n_brokers = args.brokers
    n_agencies = args.agencies

    out_dir = Path(args.out_dir) if args.out_dir else Path(tempfile.mkdtemp(prefix="dealflow-gen-"))
    out_dir.mkdir(parents=True, exist_ok=True)

    tz = pg.run("SHOW TimeZone").strip()
    if tz != "Africa/Johannesburg":
        print(f"NOTE  database TimeZone is {tz}, the build spec pins Africa/Johannesburg.")
        print("      Raw timestamps carry an explicit +02 offset so this feed is unaffected,")
        print("      but run ALTER DATABASE ... SET timezone before the SCD2 loaders.")

    t0 = time.perf_counter()
    timings: dict[str, float] = {}

    def lap(name: str, since: float) -> float:
        now = time.perf_counter()
        timings[name] = round(now - since, 3)
        print(f"    {name:34s} {timings[name]:8.3f} s")
        return now

    print(f"generating (seed {DEALFLOW_SEED}, scale {args.scale})")
    mark = t0
    months = month_starts(HISTORY_START, HISTORY_END)
    agencies = build_agencies(n_agencies)
    brokers = build_brokers(n_brokers, n_agencies, len(months))
    brokers_by_nk = {b.nk: b for b in brokers}
    props = build_properties(n_props, args.scale)
    mark = lap("entities", mark)

    dir_path = out_dir / "broker_directory.tsv"
    dir_rows = write_broker_directory(dir_path, brokers, agencies, months)
    mark = lap(f"broker_directory ({dir_rows} rows)", mark)

    prp_path = out_dir / "property_register.tsv"
    prp_rows = write_property_register(prp_path, props, months)
    mark = lap(f"property_register ({prp_rows} rows)", mark)

    counts = allocate_monthly_counts(n_deals, months)
    deals = build_deals(months, counts, brokers, props)
    mark = lap(f"deals in memory ({len(deals)})", mark)

    sub_path = out_dir / "deal_submission.tsv"
    sub_stats = write_deal_submissions(sub_path, deals, props, brokers_by_nk, args.scale)
    mark = lap(f"deal_submission ({sub_stats['rows']} rows)", mark)

    evt_paths, evt_stats = write_stage_events(out_dir, deals, args.scale)
    mark = lap(f"stage_event ({evt_stats['rows']} rows)", mark)
    gen_seconds = round(time.perf_counter() - t0, 3)

    print(f"\ngeneration wall clock: {gen_seconds} s   files in {out_dir}")
    print("injected (from the generator's own counters):")
    for k, v in sorted(sub_stats.items()):
        print(f"    submission {k:26s} {v:>9d}")
    for k, v in sorted(evt_stats.items()):
        print(f"    event      {k:26s} {v:>9d}")

    if args.no_load:
        return 0

    t_load = time.perf_counter()
    print(f"\nloading into schema {args.schema}")
    pg.script(raw_ddl(args.schema))

    # The landing tables are append only, so a second load into a schema that
    # already holds a dataset does not replace it, it doubles it, and every shape
    # metric downstream then reports on two interleaved copies. That failure is
    # silent, which is the worst kind, so it is made loud here instead: the load
    # refuses to start unless the caller has said which of the two things they
    # meant. TRUNCATE is not the default because this generator is pointed at a
    # shared database and a schema name is one typo away from somebody else's data.
    existing = {
        t: int(n) for t, n in (
            line.split("|") for line in pg.run(
                " UNION ALL ".join(
                    f"SELECT '{t}', count(*)::text FROM {args.schema}.{t}"
                    for t in RAW_COLUMNS
                )
            ).strip().splitlines() if line
        )
    }
    occupied = {t: n for t, n in existing.items() if n > 0}
    if occupied and not (args.truncate or args.append):
        print(f"\nREFUSING TO LOAD: {args.schema} already holds data:")
        for t, n in sorted(occupied.items()):
            print(f"    {args.schema}.{t:20s} {n:>10,d} rows")
        print("\nThese tables are append only, so loading again would double them.")
        print("Choose one:")
        print(f"    --truncate            empty {args.schema} first, then load (the usual choice)")
        print("    --append              deliberately add a second dataset")
        print("    --schema <other>      load somewhere else")
        return 2
    if args.truncate:
        pg.script("TRUNCATE " + ", ".join(
            f"{args.schema}.{t}" for t in RAW_COLUMNS) + ";")
    pg.copy_files(args.schema, "broker_directory", [dir_path])
    mark = lap("COPY broker_directory", t_load)
    pg.copy_files(args.schema, "property_register", [prp_path])
    mark = lap("COPY property_register", mark)
    pg.copy_files(args.schema, "deal_submission", [sub_path])
    mark = lap("COPY deal_submission", mark)
    pg.copy_files(args.schema, "stage_event", evt_paths)
    mark = lap("COPY stage_event", mark)
    pg.script(raw_index_ddl(args.schema))
    mark = lap("BRIN indexes", mark)
    pg.script(f"ANALYZE {args.schema}.deal_submission; ANALYZE {args.schema}.stage_event; "
              f"ANALYZE {args.schema}.broker_directory; ANALYZE {args.schema}.property_register;")
    lap("ANALYZE", mark)
    load_seconds = round(time.perf_counter() - t_load, 3)

    total = round(time.perf_counter() - t0, 3)
    print(f"\nload wall clock: {load_seconds} s")
    print(f"TOTAL wall clock: {total} s")

    ledger = print_report(pg, args.schema)

    if args.ledger_out:
        Path(args.ledger_out).write_text(json.dumps({
            "seed": DEALFLOW_SEED,
            "scale": args.scale,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "generation_seconds": gen_seconds,
            "load_seconds": load_seconds,
            "total_seconds": total,
            "timings": timings,
            "injection_rates": RATE,
            "injection_counts": COUNT,
            "generator_counters": {"submission": sub_stats, "event": evt_stats},
            "sql_report": ledger,
        }, indent=2) + "\n")
        print(f"\nledger written to {args.ledger_out}")

    if not args.keep_files:
        shutil.rmtree(out_dir, ignore_errors=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
