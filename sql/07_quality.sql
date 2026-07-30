-- =====================================================================
-- 07_quality.sql
-- THE DATA QUALITY ASSERTION SUITE
-- =====================================================================
-- WHAT THIS IS, AND HOW IT DIFFERS FROM THE PIPELINE GATE
--   01_staging.sql already ships dq.check_definition: 12 blocking rules,
--   each returning ONE number, executed inline by the load at three gates.
--   That is the right shape for a gate. It tells you a run must stop.
--   It does not tell you WHICH ROWS are wrong, and a number you cannot
--   drill into is a number nobody triages.
--
--   This file is the other half: an ASSERTION SUITE. Every assertion is a
--   query that returns ZERO ROWS when the warehouse is healthy and THE
--   OFFENDING ROWS when it is not. An analyst who is paged at 07:00 can
--   run one assertion and get the actual broker codes, deal references and
--   timestamps to hand to the source system owner. That is the difference
--   between "DQ-005 failed" and "these 1,385 building names do not exist
--   in the property register, here they are".
--
--   The two are deliberately complementary and share the dq schema:
--     dq.check_definition  gate      1 number   blocks the load
--     dq.assertion         audit     N rows     diagnoses the load
--
-- THE THREE THINGS THAT MAKE THIS MORE THAN A LIST OF QUERIES
--   1. Assertions are DATA, not code. Adding one is an INSERT. The runner
--      never changes, so a new rule cannot break the harness.
--   2. Assertions are SCHEMA PARAMETERISED. The token {{star}} is
--      substituted at run time, so the identical battery can be pointed at
--      mart (production), at a release candidate schema, or at a rebuild,
--      and the results compared. A rule that only works on one schema is a
--      script, not a control.
--   3. Assertions are BANDED, not binary. The verdict is
--      offending_rows BETWEEN expect_min AND expect_max. Most bands are
--      0 to 0, but some invariants are legitimately non-zero (the DEFAULT
--      partition is deliberately fed), and a control that has to be
--      disabled the first time reality disagrees with it gets disabled
--      permanently.
--
-- THE TWO BATTERIES, AND WHY THEY MEAN OPPOSITE THINGS
--   battery = 'staging'    A SUPPLIER SCORECARD. It measures the quality
--                          of what arrived. It is EXPECTED to fail on real
--                          intake data, and each failure is a finding to
--                          take back to the source system, not a bug in
--                          this warehouse. Its job is to quantify dirt.
--   battery = 'warehouse'  OUR OWN CORRECTNESS CONTRACT. Every error
--                          severity assertion here must pass. A failure
--                          means the pipeline let something through, or
--                          computed something wrong, and it is ours to fix.
--   Conflating those two is the most common reason DQ frameworks get
--   ignored: everything is red, so nobody looks.
-- =====================================================================

\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS dq;

-- ---------------------------------------------------------------------
-- THE CATALOGUE
-- ---------------------------------------------------------------------
-- CASCADE, because the two reader views at the bottom of this file depend on
-- these tables, so a re-run without it fails on the dependency rather than
-- rebuilding. The views are recreated below, so nothing is lost.
DROP TABLE IF EXISTS dq.assertion_result CASCADE;
DROP TABLE IF EXISTS dq.assertion_run    CASCADE;
DROP TABLE IF EXISTS dq.assertion        CASCADE;

CREATE TABLE dq.assertion (
    assertion_code   text    NOT NULL PRIMARY KEY,
    assertion_name   text    NOT NULL,
    battery          text    NOT NULL,
    quality_dimension text   NOT NULL,
    severity         text    NOT NULL,
    -- The business consequence, in one sentence, of this assertion being
    -- violated. An assertion that cannot state its consequence is
    -- decoration and must be deleted rather than shipped.
    protects_against text    NOT NULL,
    seeded_defect    text,
    expect_min       bigint  NOT NULL DEFAULT 0,
    expect_max       bigint  NOT NULL DEFAULT 0,
    offending_sql    text    NOT NULL,
    is_enabled       boolean NOT NULL DEFAULT true,
    CONSTRAINT ck_assertion_battery   CHECK (battery IN ('staging', 'warehouse')),
    CONSTRAINT ck_assertion_severity  CHECK (severity IN ('error', 'warn')),
    CONSTRAINT ck_assertion_dimension CHECK (quality_dimension IN
        ('referential', 'uniqueness', 'scd2', 'value_range', 'freshness',
         'volume_drift', 'dirty_data', 'reconciliation', 'planner_health')),
    CONSTRAINT ck_assertion_band      CHECK (expect_max >= expect_min)
);
COMMENT ON TABLE dq.assertion IS
'One row per data quality assertion. offending_sql returns zero rows when healthy and the offending rows when not, so a failure is immediately triageable rather than merely countable.';
COMMENT ON COLUMN dq.assertion.offending_sql IS
'A bare SELECT returning the offending rows. The runner wraps it, so it must be usable as a subquery: no trailing semicolon, no ORDER BY dependency, no CTE that references the wrapper.';
COMMENT ON COLUMN dq.assertion.battery IS
'staging is a supplier scorecard and is expected to fail on real intake data. warehouse is our own correctness contract and every error severity assertion in it must pass.';
COMMENT ON COLUMN dq.assertion.expect_min IS
'Lower bound on the offending row count. Non-zero only where a non-zero count is the CORRECT answer, for example the deliberately fed DEFAULT partition.';

CREATE TABLE dq.assertion_run (
    assertion_run_id bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    battery          text        NOT NULL,
    star_schema      text        NOT NULL,
    started_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at      timestamptz,
    duration_ms      numeric(12,1),
    assertions_run   integer,
    assertions_failed integer,
    assertions_errored integer,
    gate_verdict     text
);
COMMENT ON TABLE dq.assertion_run IS
'One row per execution of a battery. star_schema is recorded because the same battery pointed at a different schema is a different observation.';

CREATE TABLE dq.assertion_result (
    assertion_result_sk bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    assertion_run_id    bigint  NOT NULL REFERENCES dq.assertion_run (assertion_run_id),
    assertion_code      text    NOT NULL REFERENCES dq.assertion (assertion_code),
    quality_dimension   text    NOT NULL,
    severity            text    NOT NULL,
    offending_rows      bigint,
    expect_min          bigint  NOT NULL,
    expect_max          bigint  NOT NULL,
    status              text    NOT NULL,
    duration_ms         numeric(12,1),
    error_text          text,
    -- Up to a handful of real offending rows, captured as JSON. This is
    -- what turns a red light into a work item, and it is why the
    -- assertions return rows rather than counts.
    offending_sample    jsonb,
    checked_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_assertion_status CHECK (status IN ('PASS', 'FAIL', 'ERROR'))
);
COMMENT ON TABLE dq.assertion_result IS
'One row per assertion per run, append only, so every check is a trend line. status ERROR is kept distinct from FAIL: a rule that could not run is not a rule that passed, and silently treating the two alike is how a broken control reports green.';
CREATE INDEX ix_dq_assertion_result_run ON dq.assertion_result (assertion_run_id, assertion_code);

-- =====================================================================
-- THE RUNNER
-- =====================================================================
-- WHY THE EXECUTION SEMANTICS LIVE IN SQL AND NOT IN PYTHON
--   The suite must be runnable from psql alone, on a box with no Python,
--   by whoever is on call. Python owns presentation and the exit code;
--   the database owns what an assertion MEANS. One implementation, so the
--   two can never disagree about a verdict.
--
-- WHY EACH ASSERTION GETS ITS OWN EXCEPTION HANDLER
--   A suite that aborts on the first broken rule is a suite that hides
--   every rule after it. A rule referencing a table that does not exist
--   yet must record ERROR and let the other 40 run. That is the
--   difference between a control and a script.
CREATE OR REPLACE FUNCTION dq.fn_run_assertions(
    p_battery     text,
    p_star_schema text DEFAULT 'mart',
    p_sample_rows integer DEFAULT 5
) RETURNS bigint
LANGUAGE plpgsql AS $fn$
DECLARE
    v_run_id     bigint;
    v_rec        record;
    v_sql        text;
    v_count      bigint;
    v_sample     jsonb;
    v_t0         timestamptz;
    v_ms         numeric(12,1);
    v_status     text;
    v_err        text;
    v_run_t0     timestamptz := clock_timestamp();
BEGIN
    IF p_star_schema !~ '^[a-z_][a-z0-9_]*$' THEN
        -- The schema name is concatenated into dynamic SQL, so it is
        -- validated rather than trusted. A regex is cheaper than quote_ident
        -- here because it also rejects names this project would never use.
        RAISE EXCEPTION 'Refusing to run: % is not a plain schema identifier', p_star_schema;
    END IF;

    INSERT INTO dq.assertion_run (battery, star_schema)
    VALUES (p_battery, p_star_schema)
    RETURNING assertion_run_id INTO v_run_id;

    FOR v_rec IN
        SELECT * FROM dq.assertion
        WHERE is_enabled AND (p_battery = 'all' OR battery = p_battery)
        ORDER BY assertion_code
    LOOP
        v_sql    := replace(v_rec.offending_sql, '{{star}}', p_star_schema);
        v_t0     := clock_timestamp();
        v_count  := NULL;
        v_sample := NULL;
        v_err    := NULL;

        BEGIN
            EXECUTE format('SELECT count(*) FROM (%s) AS _offending', v_sql) INTO v_count;

            IF v_count NOT BETWEEN v_rec.expect_min AND v_rec.expect_max THEN
                v_status := 'FAIL';
                -- Only a failing assertion pays the cost of materialising a
                -- sample. A passing assertion has nothing to show.
                EXECUTE format(
                    'SELECT jsonb_agg(to_jsonb(_s)) FROM (SELECT * FROM (%s) AS _o LIMIT %s) AS _s',
                    v_sql, p_sample_rows) INTO v_sample;
            ELSE
                v_status := 'PASS';
            END IF;
        EXCEPTION WHEN others THEN
            v_status := 'ERROR';
            v_err    := SQLSTATE || ': ' || SQLERRM;
        END;

        v_ms := EXTRACT(EPOCH FROM (clock_timestamp() - v_t0)) * 1000;

        INSERT INTO dq.assertion_result (
            assertion_run_id, assertion_code, quality_dimension, severity,
            offending_rows, expect_min, expect_max, status, duration_ms,
            error_text, offending_sample)
        VALUES (v_run_id, v_rec.assertion_code, v_rec.quality_dimension,
                v_rec.severity, v_count, v_rec.expect_min, v_rec.expect_max,
                v_status, v_ms, v_err, v_sample);
    END LOOP;

    UPDATE dq.assertion_run r
    SET finished_at        = clock_timestamp(),
        duration_ms        = EXTRACT(EPOCH FROM (clock_timestamp() - v_run_t0)) * 1000,
        assertions_run     = s.total,
        assertions_failed  = s.failed,
        assertions_errored = s.errored,
        -- The gate verdict is driven by ERROR severity only. A warn that
        -- fires is a finding to look at, not a reason to stop a load, and
        -- a framework that cannot express that distinction gets switched
        -- off the first busy week.
        gate_verdict       = CASE WHEN s.blocking = 0 THEN 'PASS' ELSE 'FAIL' END
    FROM (
        SELECT count(*) AS total,
               count(*) FILTER (WHERE status = 'FAIL')  AS failed,
               count(*) FILTER (WHERE status = 'ERROR') AS errored,
               count(*) FILTER (WHERE status IN ('FAIL', 'ERROR') AND severity = 'error') AS blocking
        FROM dq.assertion_result WHERE assertion_run_id = v_run_id
    ) s
    WHERE r.assertion_run_id = v_run_id;

    RETURN v_run_id;
END;
$fn$;
COMMENT ON FUNCTION dq.fn_run_assertions(text, text, integer) IS
'Executes one battery against one star schema, writes dq.assertion_result, and returns the assertion_run_id. Each assertion is wrapped in its own exception handler so a rule that cannot run records ERROR instead of hiding every rule after it.';

-- =====================================================================
-- BATTERY 1: STAGING. THE SUPPLIER SCORECARD.
-- =====================================================================
-- Every assertion below expects ZERO offending rows, because a source
-- system SHOULD send parseable values, unique keys, resolvable references
-- and events in order. It does not, and the count is the size of the
-- problem. These assertions are the evidence for a conversation with the
-- source system owner, and the reason each of them is worth a query is
-- written in protects_against.
--
-- The staging battery reads raw.*, stg.*, dq.* and the conformed
-- REFERENCE dimensions (dim_stage), but never the facts. A staging control
-- that depended on the star could not run before the star was built, which
-- is exactly when it is most needed.
INSERT INTO dq.assertion
    (assertion_code, assertion_name, battery, quality_dimension, severity,
     protects_against, seeded_defect, expect_min, expect_max, offending_sql) VALUES

('ASSERT-STG-001', 'Deal value text that is not a number', 'staging', 'dirty_data', 'error',
 'A value the warehouse cannot read is pipeline value it cannot report. These rows are diverted to the reject table, so the deal silently disappears from every revenue number until someone re-keys it.',
 'D04 unparseable value string', 0, 0,
$q$
SELECT r.source_file, r.raw_line_no, r.source_message_nk, r.deal_nk,
       r.deal_value_zar AS offending_value
FROM raw.deal_submission r
WHERE r.deal_value_zar IS NOT NULL
  AND NOT util.fn_is_numeric(r.deal_value_zar)
$q$),

('ASSERT-STG-002', 'Received date that is not a real calendar date', 'staging', 'dirty_data', 'error',
 'A date like 2026-02-30 does not exist. PostgreSQL raises on it, so the whole submission is rejected, and to_date would have been worse: it silently returns 2026-03-02 and the deal enters the warehouse two days late with nothing ever complaining.',
 'D05 impossible calendar date', 0, 0,
$q$
SELECT r.source_file, r.raw_line_no, r.source_message_nk, r.deal_nk,
       r.received_ts AS offending_value
FROM raw.deal_submission r
WHERE r.received_ts IS NULL
   OR NOT util.fn_is_real_date(r.received_ts)
$q$),

('ASSERT-STG-003', 'Negative deal value in the landing zone', 'staging', 'value_range', 'error',
 'A negative pipeline value is arithmetically impossible and, if it reaches a SUM, it cancels out a real deal instead of merely being wrong. Staging repairs the sign, but an unflagged repair is indistinguishable from data, so the arrival is asserted here.',
 'D09 negative value from a credit note sign typo', 0, 0,
$q$
SELECT r.source_file, r.raw_line_no, r.deal_nk, r.deal_value_zar AS offending_value
FROM raw.deal_submission r
WHERE util.fn_is_numeric(r.deal_value_zar)
  AND r.deal_value_zar::numeric < 0
$q$),

('ASSERT-STG-004', 'Deal value implausibly small for commercial property', 'staging', 'value_range', 'warn',
 'A commercial property deal below R100,000 is almost certainly a value keyed in thousands rather than units. It parses, it loads, and it drags every average and every median down without ever failing a type check.',
 'D03 value keyed in thousands rather than units', 0, 0,
$q$
SELECT s.source_message_nk, s.deal_nk, s.broker_nk, s.deal_value_zar
FROM stg.deal_submission s
WHERE s.deal_value_zar IS NOT NULL
  AND s.deal_value_zar > 0
  AND s.deal_value_zar < 100000
$q$),

('ASSERT-STG-005', 'Deal value absent entirely', 'staging', 'value_range', 'warn',
 'A deal with no value still counts in the funnel but contributes nothing to pipeline value, so conversion rates by count and by value disagree and nobody can say which is right. Absent is deliberately NOT treated as a reject: the deal is real and must still be counted.',
 'D02 value missing entirely', 0, 0,
$q$
SELECT s.source_message_nk, s.deal_nk, s.broker_nk, s.received_date
FROM stg.deal_submission s
WHERE s.deal_value_zar IS NULL
$q$),

('ASSERT-STG-006', 'Broker email needed case or whitespace repair', 'staging', 'dirty_data', 'warn',
 'The same broker arriving as three spellings becomes three brokers in any report that groups by the raw string, which splits their book and understates the top of the leaderboard. Normalisation fixes it; this assertion measures how often it was needed.',
 'D06 broker email case and whitespace variants', 0, 0,
$q$
SELECT r.source_file, r.raw_line_no, r.deal_nk, r.broker_email AS offending_value
FROM raw.deal_submission r
WHERE r.broker_email IS NOT NULL
  AND r.broker_email <> lower(regexp_replace(r.broker_email, '\s', '', 'g'))
$q$),

('ASSERT-STG-007', 'Submitting broker is in no directory snapshot', 'staging', 'referential', 'error',
 'A broker the directory has never heard of cannot be attributed to an agency or a region, so the deal lands on the unknown member. It still counts in the total, which is correct, but it is invisible in every broker and agency breakdown until the directory is fixed.',
 'D07 broker code absent from the directory', 0, 0,
$q$
SELECT s.source_message_nk, s.deal_nk, s.broker_nk, s.received_date
FROM stg.deal_submission s
WHERE NOT EXISTS (
    SELECT 1 FROM stg.broker_directory d WHERE d.broker_nk = s.broker_nk
)
$q$),

('ASSERT-STG-008', 'Submitted building name is not in the property register', 'staging', 'referential', 'error',
 'Brokers type building names freehand, so a trailing legal suffix makes one property look like two. Left unconformed, the same building reports as two properties, sector totals double count, and the property level coverage query silently misses live deals.',
 'D08 property name variants', 0, 0,
$q$
-- Only the name only submissions are in scope: a submission that carried a
-- property code needs no name matching at all. The comparison is against the
-- NORMALISED register name, which is the same conform rule the load uses, so
-- this control fails exactly when the load would have to fall back to the
-- unknown member.
SELECT r.source_file, r.raw_line_no, r.deal_nk, r.property_name AS offending_value
FROM raw.deal_submission r
WHERE r.property_nk IS NULL
  AND NOT EXISTS (
      SELECT 1 FROM stg.property_register p
      WHERE p.property_name_norm = util.fn_norm_name(r.property_name)
  )
$q$),

('ASSERT-STG-009', 'Property register row with no sector', 'staging', 'dirty_data', 'warn',
 'Sector is the primary way this business slices its pipeline. A property with no sector drops out of every inner join that mentions sector, so the sector totals no longer add up to the grand total and the gap is invisible unless it is asserted.',
 'D13 property register rows with no sector', 0, 0,
$q$
SELECT p.raw_line_no, p.property_nk, p.effective_date, p.register_event_type
FROM raw.property_register p
WHERE p.sector IS NULL OR btrim(p.sector) = ''
$q$),

('ASSERT-STG-010', 'Same deal submitted more than once by content', 'staging', 'uniqueness', 'warn',
 'A resubmission arrives with a fresh message id and a jittered value, so neither the id nor the value can identify it. Counted twice, one deal becomes two in the funnel and its value is double counted in pipeline reporting.',
 'D01 duplicate resubmission under a fresh message id', 0, 0,
$q$
-- Content fingerprint: same broker, same property, same week. The VALUE is
-- deliberately NOT part of the key, because the resubmission jitters it by
-- up to 2% and a key that the defect can move is not a key.
SELECT s.source_message_nk, s.deal_nk, s.broker_nk, s.property_name_norm,
       s.deal_value_zar, s.received_ts
FROM (
    SELECT d.*,
           ROW_NUMBER() OVER (PARTITION BY d.broker_nk, d.property_name_norm,
                                           date_trunc('week', d.received_ts)
                              ORDER BY d.received_ts, d.raw_line_no) AS rn
    FROM stg.deal_submission d
) s
WHERE s.rn > 1
$q$),

('ASSERT-STG-011', 'Same source message id landed more than once', 'staging', 'uniqueness', 'error',
 'This is the signature of a source file ingested twice, which is the most common real pipeline accident: an operator re-runs a day, or a retry fires after a timeout that had actually succeeded. It is dangerous because the staging primary key silently absorbs the second copy and nothing raises.',
 'D18 a whole source file ingested twice', 0, 0,
$q$
SELECT r.source_message_nk, count(*) AS times_landed,
       min(r.source_file) AS first_file, max(r.raw_line_no) AS last_line
FROM raw.deal_submission r
GROUP BY r.source_message_nk
HAVING count(*) > 1
$q$),

('ASSERT-STG-012', 'Broker listed twice in one directory snapshot', 'staging', 'uniqueness', 'error',
 'A full snapshot must contain each broker once. Twice doubles any headcount metric, and worse, it would open two Type 2 dimension versions at the same instant, which the EXCLUDE constraint on the dimension would then reject and fail the whole load.',
 'D14 the same broker listed twice in one snapshot', 0, 0,
$q$
SELECT d.snapshot_date, d.broker_nk, count(*) AS rows_in_snapshot
FROM stg.broker_directory d
GROUP BY d.snapshot_date, d.broker_nk
HAVING count(*) > 1
$q$),

('ASSERT-STG-013', 'Stage code that resolves to no pipeline stage', 'staging', 'referential', 'error',
 'An unrecognised stage code cannot be placed in the funnel at all. The event is quarantined, so the deal appears to skip a stage, and time in stage for the surrounding stages is silently overstated because the gap is attributed to its neighbours.',
 'D16 unknown stage code', 0, 0,
$q$
SELECT r.source_file, r.raw_line_no, r.source_event_nk, r.deal_nk,
       r.stage_code AS offending_value
FROM raw.stage_event r
-- Resolved against dim_stage.stage_code, the dimension's NATURAL key, not
-- against a pattern this assertion invents. A control that re-implements the
-- load's mapping rule tests the control, not the load.
WHERE r.stage_code IS NULL
   OR NOT EXISTS (
       SELECT 1 FROM {{star}}.dim_stage s
       WHERE s.stage_code = upper(btrim(r.stage_code))
   )
$q$),

('ASSERT-STG-014', 'Exact duplicate stage event payload', 'staging', 'uniqueness', 'error',
 'The same deal, stage and timestamp arriving under two event ids double counts a single transition. Every funnel conversion rate is computed from these counts, so a 2% duplicate rate is a 2% lie in every stage to stage percentage on the dashboard.',
 'D17 exact duplicate stage event payloads', 0, 0,
$q$
SELECT e.deal_nk, e.to_stage_sk, e.event_ts, count(*) AS payloads,
       min(e.event_nk) AS kept_event_nk, max(e.event_nk) AS duplicate_event_nk
FROM stg.stage_event e
GROUP BY e.deal_nk, e.to_stage_sk, e.event_ts
HAVING count(*) > 1
$q$),

('ASSERT-STG-015', 'Stage events for a deal that was never submitted', 'staging', 'referential', 'error',
 'An event with no deal behind it has no broker, no property and no value. Loaded, it inflates the funnel with phantom deals; quarantined, it means the intake feed and the pipeline feed disagree about which deals exist, which is a source system integration fault worth escalating.',
 'D12 events for a deal reference never successfully submitted', 0, 0,
$q$
SELECT e.event_nk, e.deal_nk, e.stage_nk, e.event_ts
FROM stg.stage_event e
WHERE NOT EXISTS (
    SELECT 1 FROM stg.deal_submission s WHERE s.deal_nk = e.deal_nk
)
$q$),

('ASSERT-STG-016', 'Stage event dated before its own deal arrived', 'staging', 'value_range', 'error',
 'A deal cannot move through a stage before it was received. This is clock skew between the intake system and the pipeline system, and it produces NEGATIVE time in stage, which drags the average cycle time down and makes the pipeline look faster than it is.',
 'D10 stage events arriving before their own deal', 0, 0,
$q$
SELECT e.event_nk, e.deal_nk, e.event_ts, s.received_ts,
       round(EXTRACT(EPOCH FROM (s.received_ts - e.event_ts)) / 86400.0, 2) AS days_early
FROM stg.stage_event e
JOIN (
    SELECT deal_nk, min(received_ts) AS received_ts
    FROM stg.deal_submission GROUP BY deal_nk
) s ON s.deal_nk = e.deal_nk
WHERE e.event_ts < s.received_ts
$q$),

('ASSERT-STG-017', 'Transition out of a terminal stage', 'staging', 'dirty_data', 'error',
 'Once a deal is Closed won, Lost or Stalled there is no business process that moves it to Offer. Such a row is a source system fault, and averaged into a funnel it invents conversions that never happened, which is worse than a missing row because it is invisible in a total.',
 'D11 physically impossible transition', 0, 0,
$q$
-- from_stage does not exist in staging, so it is derived here with the same
-- LAG the int layer uses. raw_line_no is the tiebreaker because the feed is
-- not guaranteed to arrive in event order and two events can share a second.
WITH sequenced AS (
    SELECT e.event_nk, e.deal_nk, e.to_stage_sk, e.event_ts,
           LAG(e.to_stage_sk) OVER (PARTITION BY e.deal_nk
                                    ORDER BY e.event_ts, e.raw_line_no) AS from_stage_sk
    FROM stg.stage_event e
)
SELECT q.event_nk, q.deal_nk, sf.stage_name AS from_stage, st.stage_name AS to_stage, q.event_ts
FROM sequenced q
JOIN {{star}}.dim_stage sf ON sf.stage_sk = q.from_stage_sk
JOIN {{star}}.dim_stage st ON st.stage_sk = q.to_stage_sk
WHERE sf.is_terminal
$q$),

('ASSERT-STG-018', 'Event date outside the declared partition range', 'staging', 'value_range', 'error',
 'An event dated 2031 has nowhere correct to live. It lands in the DEFAULT partition, which is deliberate rather than accidental, but every out of range date is a source clock or parsing fault and a DEFAULT partition allowed to grow unwatched eventually holds a year of data nobody queries.',
 'D15 event dates outside the declared partition range', 0, 0,
$q$
-- The declared range is read from the partitions that actually exist rather
-- than hardcoded, so extending the fact by a year does not silently turn
-- this control into a false alarm.
WITH declared AS (
    SELECT min(to_date(right(c.relname, 7), 'YYYY_MM'))                            AS range_start,
           max(to_date(right(c.relname, 7), 'YYYY_MM') + interval '1 month')::date AS range_end
    FROM pg_class c
    JOIN pg_inherits i ON i.inhrelid = c.oid
    WHERE i.inhparent = '{{star}}.fct_deal_stage_event'::regclass
      AND c.relname ~ '_[0-9]{4}_[0-9]{2}$'
)
SELECT e.event_nk, e.deal_nk, e.event_ts, d.range_start, d.range_end
FROM stg.stage_event e CROSS JOIN declared d
WHERE e.event_ts::date < d.range_start
   OR e.event_ts::date >= d.range_end
$q$),

('ASSERT-STG-019', 'Raw row that reached neither staging nor a reject table', 'staging', 'reconciliation', 'error',
 'Error tolerant loading only works if every diverted row is accounted for. A raw row that is in no downstream table at all was lost silently, which is the single worst outcome of a reject-and-continue design and the reason reconciliation is a control and not a nicety.',
 'D18 a whole source file ingested twice, silently absorbed by the staging primary key', 0, 0,
$q$
-- Deliberately keyed on (source_file, raw_line_no), the ADDRESS of a raw
-- row, and not on the message id. Keying on the id would make the double
-- ingest reconcile perfectly, which is precisely the bug being hunted.
SELECT r.source_file, r.raw_line_no, r.source_message_nk, r.deal_nk
FROM raw.deal_submission r
WHERE NOT EXISTS (
        SELECT 1 FROM stg.deal_submission s
        WHERE s.source_file = r.source_file AND s.raw_line_no = r.raw_line_no)
  AND NOT EXISTS (
        SELECT 1 FROM dq.reject_deal_submission j
        WHERE j.source_file = r.source_file AND j.raw_line_no = r.raw_line_no)
$q$),

('ASSERT-STG-020', 'Source feed has stopped arriving', 'staging', 'freshness', 'error',
 'This is the defect no row level rule can find, because every row that IS present is perfectly valid. A delta feed that dies leaves the dimension frozen: property reclassifications stop being applied, and every deal after that date is reported under a stale sector while all row counts look healthy.',
 'D19 the property register delta feed stopped and nobody noticed', 0, 0,
$q$
-- Tolerances are per stream because the feeds have different cadences. A
-- monthly snapshot is not late at 20 days; a daily event feed is.
WITH expectation (stream_nk, max_age_days) AS (
    VALUES ('raw.deal_submission', 3), ('raw.stage_event', 3),
           ('raw.broker_directory', 45), ('raw.property_register', 45)
),
observed AS (
    -- Future dated rows are excluded from the freshness measure. A feed is
    -- not fresh because it contains a row dated 2031; that is a separate
    -- defect, and ASSERT-STG-018 owns it.
    SELECT 'raw.deal_submission'  AS stream_nk, max(ingested_at) AS latest_row FROM raw.deal_submission  WHERE ingested_at <= now()
    UNION ALL SELECT 'raw.stage_event',       max(ingested_at) FROM raw.stage_event      WHERE ingested_at <= now()
    UNION ALL SELECT 'raw.broker_directory',  max(ingested_at) FROM raw.broker_directory WHERE ingested_at <= now()
    UNION ALL SELECT 'raw.property_register', max(ingested_at) FROM raw.property_register WHERE ingested_at <= now()
)
SELECT e.stream_nk, o.latest_row, e.max_age_days,
       (now()::date - o.latest_row::date) AS actual_age_days
FROM expectation e
LEFT JOIN observed o ON o.stream_nk = e.stream_nk
WHERE o.latest_row IS NULL
   OR (now()::date - o.latest_row::date) > e.max_age_days
$q$),

('ASSERT-STG-021', 'Monthly submission volume broke its seasonal expectation', 'staging', 'volume_drift', 'error',
 'A month that doubles is usually a double ingest and a month that halves is usually a truncated file, and both load without a single row level error. The comparison is against the SAME MONTH ONE YEAR EARLIER rather than the previous month, because this market genuinely collapses every December and a month on month rule would cry wolf every January.',
 'D18 a whole source file ingested twice', 0, 0,
$q$
WITH monthly AS (
    SELECT date_trunc('month', r.ingested_at)::date AS ingest_month, count(*) AS rows_ingested
    FROM raw.deal_submission r
    WHERE r.ingested_at <= now()
      -- The current month is partial by definition, so comparing it would
      -- report a false collapse on every day except the last of the month.
      AND date_trunc('month', r.ingested_at) < date_trunc('month', now())
    GROUP BY 1
)
SELECT c.ingest_month, c.rows_ingested, p.rows_ingested AS rows_same_month_last_year,
       round(100.0 * (c.rows_ingested - p.rows_ingested) / p.rows_ingested, 1) AS pct_change
FROM monthly c
JOIN monthly p ON p.ingest_month = (c.ingest_month - interval '1 year')::date
WHERE abs(c.rows_ingested - p.rows_ingested) > 0.40 * p.rows_ingested
$q$);

-- =====================================================================
-- BATTERY 2: WAREHOUSE. OUR OWN CORRECTNESS CONTRACT.
-- =====================================================================
-- Every error severity assertion here must pass. A failure is not a
-- supplier problem, it is a defect in this warehouse: something the
-- pipeline let through, resolved to the wrong dimension version, routed to
-- the wrong partition, or computed wrongly.
--
-- {{star}} is substituted at run time so the identical battery can be run
-- against mart, against a rebuild, or against a release candidate schema,
-- and the two result sets compared row for row.
INSERT INTO dq.assertion
    (assertion_code, assertion_name, battery, quality_dimension, severity,
     protects_against, seeded_defect, expect_min, expect_max, offending_sql) VALUES

('ASSERT-WH-001', 'More than one current version per broker', 'warehouse', 'scd2', 'error',
 'Two current rows for one broker means every join to the current dimension DOUBLES that broker''s deals. It is the classic Type 2 failure and it inflates a leaderboard silently, because the total still looks plausible.',
 NULL, 0, 0,
$q$
SELECT b.broker_nk, count(*) AS current_versions,
       min(b.valid_from) AS earliest_open, max(b.valid_from) AS latest_open
FROM {{star}}.dim_broker b
WHERE b.is_current
GROUP BY b.broker_nk
HAVING count(*) > 1
$q$),

('ASSERT-WH-002', 'Overlapping validity windows on dim_broker', 'warehouse', 'scd2', 'error',
 'Overlapping windows mean a fact dated inside the overlap matches TWO dimension versions, so the point in time join fans out and double counts. The EXCLUDE constraint makes this impossible; this assertion is the independent second opinion that proves the constraint is actually present and enforced, which a constraint dropped during a backfill would not be.',
 NULL, 0, 0,
$q$
-- Written as an explicit self join rather than trusting the constraint. A
-- control that is implemented by the thing it is checking is not a control.
SELECT a.broker_nk, a.broker_sk AS sk_a, b.broker_sk AS sk_b,
       a.valid_from AS from_a, a.valid_to AS to_a,
       b.valid_from AS from_b, b.valid_to AS to_b
FROM {{star}}.dim_broker a
JOIN {{star}}.dim_broker b
  ON b.broker_nk = a.broker_nk
 AND b.broker_sk > a.broker_sk
 AND tstzrange(a.valid_from, a.valid_to) && tstzrange(b.valid_from, b.valid_to)
$q$),

('ASSERT-WH-003', 'Gap in a broker''s validity coverage', 'warehouse', 'scd2', 'error',
 'A gap means a fact dated inside it matches NO version, so the deal silently falls out of the point in time join and vanishes from the warehouse without changing a single row count. Non overlap alone is not enough: the windows must also be contiguous.',
 NULL, 0, 0,
$q$
SELECT v.broker_nk, v.valid_to AS gap_starts, v.next_valid_from AS gap_ends,
       v.next_valid_from - v.valid_to AS gap_length
FROM (
    SELECT b.broker_nk, b.valid_to,
           LEAD(b.valid_from) OVER (PARTITION BY b.broker_nk ORDER BY b.valid_from) AS next_valid_from
    FROM {{star}}.dim_broker b
) v
WHERE v.next_valid_from IS NOT NULL
  AND v.next_valid_from <> v.valid_to
$q$),

('ASSERT-WH-004', 'is_current disagrees with valid_to', 'warehouse', 'scd2', 'error',
 'The most common Type 2 defect in production is is_current drifting out of step with valid_to after a manual backfill, which makes "as at today" and "as at now()" return different answers. It is unreachable here because is_current is a GENERATED STORED column, and this assertion is what proves the column is still generated rather than having been rewritten as a plain boolean.',
 NULL, 0, 0,
$q$
SELECT b.broker_sk, b.broker_nk, b.valid_from, b.valid_to, b.is_current
FROM {{star}}.dim_broker b
WHERE b.is_current <> (b.valid_to = 'infinity'::timestamptz)
$q$),

('ASSERT-WH-005', 'Broker history starts later than the broker appears in the feed', 'warehouse', 'scd2', 'error',
 'A broker present in the first directory snapshot has history that predates the feed, so their first version must be anchored at minus infinity. Anchored at the snapshot date instead, any earlier fact drops out of the point in time join and disappears without changing a row count. A genuine later joiner correctly anchors at their first snapshot, so the test is against the feed and not against a constant.',
 NULL, 0, 0,
$q$
SELECT d.broker_nk, d.first_seen_in_feed, v.first_version_from
FROM (
    SELECT broker_nk, min(snapshot_date) AS first_seen_in_feed
    FROM stg.broker_directory GROUP BY broker_nk
) d
JOIN (
    SELECT broker_nk, min(valid_from) AS first_version_from
    FROM {{star}}.dim_broker WHERE broker_sk <> -1 GROUP BY broker_nk
) v ON v.broker_nk = d.broker_nk
WHERE v.first_version_from > d.first_seen_in_feed::timestamptz
$q$),

('ASSERT-WH-006', 'More than one current version per property', 'warehouse', 'scd2', 'error',
 'The same doubling risk as the broker dimension, on the axis this business slices by most. Two current rows for one property double every sector and province total that joins to the current dimension.',
 NULL, 0, 0,
$q$
SELECT p.property_nk, count(*) AS current_versions
FROM {{star}}.dim_property p
WHERE p.is_current
GROUP BY p.property_nk
HAVING count(*) > 1
$q$),

('ASSERT-WH-007', 'Overlapping validity windows on dim_property', 'warehouse', 'scd2', 'error',
 'A property reclassified from Industrial to Mixed use must have exactly one sector at any instant. Overlapping windows mean a 2021 deal matches both the old and the new sector, so it is counted in two sectors at once and the sector split no longer sums to the total.',
 NULL, 0, 0,
$q$
SELECT a.property_nk, a.property_sk AS sk_a, b.property_sk AS sk_b,
       a.valid_from AS from_a, a.valid_to AS to_a,
       b.valid_from AS from_b, b.valid_to AS to_b
FROM {{star}}.dim_property a
JOIN {{star}}.dim_property b
  ON b.property_nk = a.property_nk
 AND b.property_sk > a.property_sk
 AND tstzrange(a.valid_from, a.valid_to) && tstzrange(b.valid_from, b.valid_to)
$q$),

('ASSERT-WH-008', 'Dimension missing its unknown member', 'warehouse', 'referential', 'error',
 'The unknown member is what lets an unresolvable fact reconcile instead of vanishing. Without it the load has only two options: reject the fact, which loses a real deal, or leave the key NULL, which drops it from every inner join. Either way warehouse totals stop matching the raw feed.',
 NULL, 0, 0,
$q$
SELECT 'dim_broker' AS dimension WHERE NOT EXISTS (SELECT 1 FROM {{star}}.dim_broker   WHERE broker_sk   = -1)
UNION ALL
SELECT 'dim_property'          WHERE NOT EXISTS (SELECT 1 FROM {{star}}.dim_property WHERE property_sk = -1)
UNION ALL
SELECT 'dim_agency'            WHERE NOT EXISTS (SELECT 1 FROM {{star}}.dim_agency   WHERE agency_sk   = -1)
UNION ALL
SELECT 'dim_deal_source'       WHERE NOT EXISTS (SELECT 1 FROM {{star}}.dim_deal_source WHERE deal_source_sk = -1)
$q$),

('ASSERT-WH-009', 'Fact resolved to a broker version not in force at the time', 'warehouse', 'referential', 'error',
 'The foreign key guarantees the broker key EXISTS. Only this assertion guarantees it was the version in force when the event happened, which is the entire point of a Type 2 dimension and the one thing a foreign key cannot check. Get it wrong and a 2021 deal is credited to the agency the broker joined in 2024.',
 NULL, 0, 0,
$q$
SELECT f.deal_event_sk, f.deal_nk, f.event_ts, b.broker_nk,
       b.valid_from, b.valid_to
FROM {{star}}.fct_deal_stage_event f
JOIN {{star}}.dim_broker b ON b.broker_sk = f.broker_sk
WHERE b.broker_sk <> -1
  AND NOT (f.event_ts >= b.valid_from AND f.event_ts < b.valid_to)
$q$),

('ASSERT-WH-010', 'Fact resolved to a property version not in force at the time', 'warehouse', 'referential', 'error',
 'A property rezoned from Industrial to Mixed use in 2024 must still report a 2021 deal as Industrial. Resolving to the current version instead retroactively rewrites history, so last year''s published sector report can no longer be reproduced.',
 NULL, 0, 0,
$q$
SELECT f.deal_event_sk, f.deal_nk, f.event_ts, p.property_nk,
       p.sector, p.valid_from, p.valid_to
FROM {{star}}.fct_deal_stage_event f
JOIN {{star}}.dim_property p ON p.property_sk = f.property_sk
WHERE p.property_sk <> -1
  AND NOT (f.event_ts >= p.valid_from AND f.event_ts < p.valid_to)
$q$),

('ASSERT-WH-011', 'Fact event date has no row in the date dimension', 'warehouse', 'referential', 'error',
 'Every reporting query joins the fact to dim_date for month, quarter and prior year. A date with no dimension row drops that day out of every trended report, so a month can quietly report on 29 of its 31 days.',
 NULL, 0, 0,
$q$
-- Scoped to the DECLARED partition range. A row in the DEFAULT partition
-- carries a mistyped year that no calendar dimension will ever hold, and
-- asserting otherwise would demand a dim_date row for the year 2031.
-- DQ-012 and ASSERT-STG-018 own those rows.
SELECT DISTINCT f.event_date, f.event_date_sk
FROM {{star}}.fct_deal_stage_event f
CROSS JOIN {{star}}.v_fact_declared_range r
WHERE f.event_date >= r.range_start
  AND f.event_date <  r.range_end
  AND NOT EXISTS (
    SELECT 1 FROM {{star}}.dim_date d WHERE d.date_actual = f.event_date
)
$q$),

('ASSERT-WH-012', 'Accumulating snapshot holds more than one row per deal', 'warehouse', 'uniqueness', 'error',
 'The stated grain is one row per deal. Two rows double the deal in every cohort and win rate calculation. This is the exact failure that partitioning this table by received_date would have made unavoidable, because PostgreSQL requires the partition key in every unique index and so deal_nk alone could never have been enforced.',
 NULL, 0, 0,
$q$
SELECT p.deal_nk, count(*) AS rows_for_deal
FROM {{star}}.fct_deal_pipeline p
GROUP BY p.deal_nk
HAVING count(*) > 1
$q$),

('ASSERT-WH-013', 'Event fact holds a deal the accumulating snapshot does not', 'warehouse', 'referential', 'error',
 'The two facts must agree on which deals exist. A deal with events but no snapshot row appears in the funnel and disappears from the cycle time and win rate reports, so the two dashboards disagree and neither can be trusted.',
 NULL, 0, 0,
$q$
SELECT DISTINCT f.deal_nk
FROM {{star}}.fct_deal_stage_event f
WHERE NOT EXISTS (
    SELECT 1 FROM {{star}}.fct_deal_pipeline p WHERE p.deal_nk = f.deal_nk
)
$q$),

-- WHY THIS ONE IS A BANDED warn AND EVERY OTHER REFERENTIAL CONTROL IS A
-- ZERO error. It was written as a zero error and it FAILED at 238 rows, and
-- the investigation is more useful than the assertion was.
--
--   The 238 are resubmissions that the dedupe could not group with their
--   original, because the defect landed ON THE BLOCKING KEY ITSELF. The key
--   is (broker, property, ISO week). When one row of a pair has its broker
--   code mangled to a code the directory has never held, the two rows fall
--   into two different blocks and both survive. The original keeps the deal's
--   whole event history; the survivor of the second block has none, because
--   the pipeline system only ever emitted events under the original's key.
--
--   Blocking CANNOT catch that by construction. Any single blocking key is
--   blind to a defect in one of its own components, and that is a property of
--   the technique, not a bug in this implementation.
--
--   THE FIX, PRICED: a second blocking pass on (property, ISO week) would
--   catch the broker mangled pairs, and a third on (broker, ISO week) the
--   property mangled ones. Multi-pass blocking then needs transitive
--   clustering across the passes, which in PostgreSQL means a recursive CTE
--   over the candidate pairs, and DQ-003 would have to assert on a cluster id
--   rather than on a key. That is the right design at a volume where 0.1
--   percent matters. It is not built here, the residual is measured, and the
--   band is what stops it drifting: 238 today, alarm past 400.
('ASSERT-WH-014', 'Accumulating snapshot holds a deal with no events', 'warehouse', 'referential', 'warn',
 'A deal in the snapshot with no events is counted in cohort totals but contributes nothing to the funnel, so every stage to stage conversion rate is quietly optimistic by that fraction. On this data the population is known and bounded: resubmissions whose blocking key was itself corrupted, which single pass blocking cannot group. The band is the control. A jump means either the source defect rate moved or the dedupe regressed, and both are worth a look.',
 'D01 duplicate resubmission, in the case where D07 also mangled the broker code on one row of the pair', 0, 400,
$q$
SELECT p.deal_nk, p.received_date, p.current_stage_sk, p.deal_value_zar
FROM {{star}}.fct_deal_pipeline p
WHERE NOT EXISTS (
    SELECT 1 FROM {{star}}.fct_deal_stage_event f WHERE f.deal_nk = p.deal_nk
)
$q$),

('ASSERT-WH-015', 'Negative money in the star', 'warehouse', 'value_range', 'error',
 'A negative value does not merely report wrongly, it CANCELS a real deal inside a SUM, so total pipeline value is understated by twice the offending amount and the error is invisible in the total. Staging repairs the sign; this proves the repair held all the way through.',
 'D09 negative value from a credit note sign typo', 0, 0,
$q$
SELECT 'fct_deal_stage_event' AS table_name, f.deal_nk, f.deal_value_zar::text AS value_found
FROM {{star}}.fct_deal_stage_event f WHERE f.deal_value_zar < 0
UNION ALL
SELECT 'fct_deal_pipeline', p.deal_nk, p.deal_value_zar::text
FROM {{star}}.fct_deal_pipeline p WHERE p.deal_value_zar < 0
$q$),

('ASSERT-WH-016', 'Time in stage negative or beyond any plausible ceiling', 'warehouse', 'value_range', 'error',
 'days_in_prev_stage is the fully additive measure the whole funnel analysis rests on. Negative values pull the average cycle time below reality, and a value above roughly five years is a timestamp fault rather than a slow deal. Either way the median and p90 time in stage stop meaning anything.',
 'D10 stage events arriving out of order', 0, 0,
$q$
-- Scoped to the DECLARED range for the same reason as ASSERT-WH-011: an
-- event thrown to 2031 produces a dwell of about 1,800 days, and that is a
-- restatement of the out of range defect rather than an independent finding.
-- Unscoped, this control would report the same 369 rows three times over.
SELECT f.deal_event_sk, f.deal_nk, f.event_ts, f.days_in_prev_stage
FROM {{star}}.fct_deal_stage_event f
CROSS JOIN {{star}}.v_fact_declared_range r
WHERE f.event_date >= r.range_start
  AND f.event_date <  r.range_end
  AND f.days_in_prev_stage IS NOT NULL
  AND (f.days_in_prev_stage < 0 OR f.days_in_prev_stage > 2000)
$q$),

('ASSERT-WH-017', 'Impossible transition reached the star', 'warehouse', 'dirty_data', 'error',
 'A transition out of Closed won, Lost or Stalled invents a conversion that never happened. Because it ADDS rows to a funnel rather than removing them, it makes the pipeline look better than it is, which is the direction of error a business will act on before it questions it.',
 'D11 physically impossible transition, quarantined in int so this proves the quarantine held', 0, 0,
$q$
SELECT f.deal_event_sk, f.deal_nk, sf.stage_name AS from_stage,
       st.stage_name AS to_stage, f.event_ts
FROM {{star}}.fct_deal_stage_event f
JOIN {{star}}.dim_stage sf ON sf.stage_sk = f.from_stage_sk
JOIN {{star}}.dim_stage st ON st.stage_sk = f.to_stage_sk
WHERE sf.is_terminal
$q$),

('ASSERT-WH-018', 'Fact event precedes its own deal receipt', 'warehouse', 'value_range', 'error',
 'The accumulating snapshot says the deal arrived on a date the event fact contradicts. The two facts must tell one story about the same deal, or cycle time measured from the snapshot and cycle time measured from the events give different answers to the same question.',
 'D10 stage events arriving out of order, quarantined in int', 0, 0,
$q$
SELECT f.deal_event_sk, f.deal_nk, f.event_ts, p.received_ts
FROM {{star}}.fct_deal_stage_event f
JOIN {{star}}.fct_deal_pipeline p ON p.deal_nk = f.deal_nk
WHERE f.event_ts < p.received_ts
$q$),

('ASSERT-WH-019', 'Milestone timestamps out of chronological order', 'warehouse', 'value_range', 'error',
 'An accumulating snapshot is only useful if its milestones advance monotonically. Qualified before received, or an offer before qualification, produces a negative days_to_offer, and a negative lag in a cohort average is the kind of number that gets a whole report thrown out.',
 NULL, 0, 0,
$q$
SELECT p.deal_nk, p.received_ts, p.qualified_ts, p.offer_ts,
       p.closed_won_ts, p.lost_ts, p.stalled_ts
FROM {{star}}.fct_deal_pipeline p
WHERE (p.qualified_ts  IS NOT NULL AND p.qualified_ts  < p.received_ts)
   OR (p.offer_ts      IS NOT NULL AND p.qualified_ts IS NOT NULL AND p.offer_ts < p.qualified_ts)
   OR (p.closed_won_ts IS NOT NULL AND p.closed_won_ts < p.received_ts)
   OR (p.lost_ts       IS NOT NULL AND p.lost_ts       < p.received_ts)
   OR (p.stalled_ts    IS NOT NULL AND p.stalled_ts    < p.received_ts)
$q$),

('ASSERT-WH-020', 'Outcome flags are not mutually exclusive', 'warehouse', 'value_range', 'error',
 'Exactly one of open, won, lost and stalled must be true. Two true double counts the deal across two outcome buckets; none true makes it vanish from all of them. Either way the outcome split stops summing to the deal count, which is the first thing anyone checks and the last thing anyone expects to be wrong.',
 NULL, 0, 0,
$q$
SELECT p.deal_nk, p.is_open, p.is_won, p.is_lost, p.is_stalled,
       (p.is_open::int + p.is_won::int + p.is_lost::int + p.is_stalled::int) AS flags_set
FROM {{star}}.fct_deal_pipeline p
WHERE (p.is_open::int + p.is_won::int + p.is_lost::int + p.is_stalled::int) <> 1
$q$),

('ASSERT-WH-021', 'Outcome flag disagrees with its milestone timestamp', 'warehouse', 'value_range', 'error',
 'A won deal with no closed_won_ts, or a closed_won_ts on a deal not marked won, means the flag and the timestamp were derived independently and have drifted apart. Two sources of truth for the same fact is how a win rate and a win date report end up disagreeing.',
 NULL, 0, 0,
$q$
SELECT p.deal_nk, p.is_won, p.closed_won_ts, p.is_lost, p.lost_ts,
       p.is_stalled, p.stalled_ts
FROM {{star}}.fct_deal_pipeline p
WHERE (p.is_won     AND p.closed_won_ts IS NULL)
   OR (NOT p.is_won AND p.closed_won_ts IS NOT NULL)
   OR (p.is_lost    AND p.lost_ts       IS NULL)
   OR (p.is_stalled AND p.stalled_ts    IS NULL)
$q$),

('ASSERT-WH-022', 'Fact row sitting in the wrong monthly partition', 'warehouse', 'referential', 'error',
 'Partition pruning is only correct if routing is correct. A row in the wrong partition is silently EXCLUDED from any query whose date filter prunes to the partition it should have been in, so a month can report short and the missing rows are still physically present, which makes the discrepancy very hard to find.',
 NULL, 0, 0,
$q$
-- tableoid gives each row the partition it is physically stored in, so the
-- declared bound and the actual content can be compared in one pass over
-- the parent with no dynamic SQL per partition.
SELECT f.deal_event_sk, f.event_date, c.relname AS stored_in_partition,
       to_date(right(c.relname, 7), 'YYYY_MM') AS partition_starts
FROM {{star}}.fct_deal_stage_event f
JOIN pg_class c ON c.oid = f.tableoid
WHERE c.relname ~ '_[0-9]{4}_[0-9]{2}$'
  AND f.event_date NOT BETWEEN to_date(right(c.relname, 7), 'YYYY_MM')
                           AND (to_date(right(c.relname, 7), 'YYYY_MM') + interval '1 month' - interval '1 day')::date
$q$),

('ASSERT-WH-023', 'DEFAULT partition holding an in range date', 'warehouse', 'referential', 'error',
 'The DEFAULT partition is a deliberate landing zone for genuinely out of range dates, so its depth is monitored rather than forbidden. But a row whose date IS inside the declared range has no business being there: that is a routing bug or a missing partition, and it will be missed by every pruned query.',
 NULL, 0, 0,
$q$
WITH declared AS (
    SELECT min(to_date(right(c.relname, 7), 'YYYY_MM'))                            AS range_start,
           max(to_date(right(c.relname, 7), 'YYYY_MM') + interval '1 month')::date AS range_end
    FROM pg_class c
    JOIN pg_inherits i ON i.inhrelid = c.oid
    WHERE i.inhparent = '{{star}}.fct_deal_stage_event'::regclass
      AND c.relname ~ '_[0-9]{4}_[0-9]{2}$'
)
SELECT f.deal_event_sk, f.deal_nk, f.event_date, d.range_start, d.range_end
FROM {{star}}.fct_deal_stage_event f
JOIN pg_class c ON c.oid = f.tableoid
CROSS JOIN declared d
WHERE c.relname LIKE '%\_default'
  AND f.event_date >= d.range_start
  AND f.event_date <  d.range_end
$q$),

('ASSERT-WH-024', 'Derived date key disagrees with its own date', 'warehouse', 'value_range', 'error',
 'event_date_sk is a GENERATED STORED column precisely so a loader bug cannot land a fact on the wrong day of the date dimension. This assertion is what proves the column is still generated: rewritten as a plain integer and populated by the loader, this is exactly the defect that would appear, and it would misdate facts without breaking any join.',
 NULL, 0, 0,
$q$
SELECT f.deal_event_sk, f.event_date, f.event_date_sk,
       (EXTRACT(YEAR FROM f.event_date) * 10000
      + EXTRACT(MONTH FROM f.event_date) * 100
      + EXTRACT(DAY FROM f.event_date))::integer AS expected_date_sk
FROM {{star}}.fct_deal_stage_event f
WHERE f.event_date_sk <> (EXTRACT(YEAR FROM f.event_date) * 10000
                        + EXTRACT(MONTH FROM f.event_date) * 100
                        + EXTRACT(DAY FROM f.event_date))::integer
$q$),

('ASSERT-WH-025', 'Duplicate transition survived into the event fact', 'warehouse', 'uniqueness', 'error',
 'One deal cannot move stage twice at the same instant. A surviving duplicate double counts a transition, and because every funnel percentage is a ratio of these counts, a small duplicate rate becomes a wrong number on every stage in the report.',
 'D17 exact duplicate stage event payloads, suppressed in int so this proves the dedupe held', 0, 0,
$q$
SELECT f.deal_nk, f.to_stage_sk, f.event_ts, count(*) AS rows_in_fact
FROM {{star}}.fct_deal_stage_event f
GROUP BY f.deal_nk, f.to_stage_sk, f.event_ts
HAVING count(*) > 1
$q$),

('ASSERT-WH-026', 'Stage dimension order is duplicated or gapped', 'warehouse', 'uniqueness', 'error',
 'Every funnel query orders stages by stage_order rather than hardcoding names, which is what stops the report breaking when a stage is renamed. A duplicate order makes the funnel sequence ambiguous and a gap makes a stage silently sort to the wrong place.',
 NULL, 0, 0,
$q$
SELECT s.stage_order, count(*) AS stages_at_this_order,
       string_agg(s.stage_name, ', ' ORDER BY s.stage_name) AS stage_names
FROM {{star}}.dim_stage s
WHERE s.stage_sk <> 0
GROUP BY s.stage_order
HAVING count(*) > 1
$q$),

('ASSERT-WH-027', 'Warehouse is stale against the landing zone', 'warehouse', 'freshness', 'error',
 'The landing zone has data the star does not. A dashboard reading the star reports last week as if it were complete, and nobody can tell the difference between a quiet week and a load that stopped. An empty star fails this deliberately: infinitely stale is the correct verdict, not a pass by vacuous truth.',
 NULL, 0, 0,
$q$
WITH landed AS (
    SELECT max(e.event_ts)::date AS latest_landed
    FROM stg.stage_event e
    WHERE e.event_ts <= now()
),
served AS (
    SELECT max(f.event_date) AS latest_served
    FROM {{star}}.fct_deal_stage_event f
)
SELECT l.latest_landed, s.latest_served,
       (l.latest_landed - coalesce(s.latest_served, '1900-01-01'::date)) AS days_behind
FROM landed l CROSS JOIN served s
WHERE l.latest_landed IS NOT NULL
  AND (s.latest_served IS NULL OR l.latest_landed - s.latest_served > 2)
$q$),

('ASSERT-WH-028', 'Fact month volume broke its seasonal expectation', 'warehouse', 'volume_drift', 'error',
 'A month that swings more than 60% against the same month a year earlier is a partial load, a double load or a partition that was never attached. None of those raise an error, and all of them are invisible in a total that is still large. Compared year on year, not month on month, because this market genuinely collapses every December.',
 NULL, 0, 0,
$q$
WITH monthly AS (
    SELECT date_trunc('month', f.event_date)::date AS event_month, count(*) AS events
    FROM {{star}}.fct_deal_stage_event f
    CROSS JOIN {{star}}.v_fact_declared_range r
    -- In range only. Without this the out of range rows supply the maximum
    -- month, so the "exclude the partial month" rule below would exclude
    -- 2031-12 and leave the genuinely partial current month being compared.
    WHERE f.event_date >= r.range_start
      AND f.event_date <  r.range_end
    GROUP BY 1
), bounded AS (
    -- The most recent month present is partial by definition, so it is
    -- excluded rather than reported as a collapse.
    SELECT m.*, min(m.event_month) OVER () AS first_month
    FROM monthly m
    WHERE m.event_month < (SELECT max(event_month) FROM monthly)
)
SELECT c.event_month, c.events, p.events AS events_same_month_last_year,
       round(100.0 * (c.events - p.events) / p.events, 1) AS pct_change
FROM bounded c
JOIN bounded p ON p.event_month = (c.event_month - interval '1 year')::date
-- THE BASELINE MUST NOT BE A RAMP, AND THIS WAS MEASURED. January 2021 held
-- 8,871 events against 3,922 in January 2020, a 126 percent rise that this
-- control reported as volume drift. It is not drift: January 2020 is the
-- first month of history, so it holds only the deals submitted in it and
-- none of the in flight backlog every later January has. The first year of a
-- warehouse's history is a ramp, not a baseline, so a comparison whose
-- BASELINE month falls inside it is not a valid comparison.
WHERE p.event_month >= (p.first_month + interval '12 months')::date
  AND abs(c.events - p.events) > 0.60 * p.events
$q$),

('ASSERT-WH-029', 'Non-empty fact partition with no extended statistics', 'warehouse', 'planner_health', 'warn',
 'from_stage_sk and to_stage_sk are near functionally dependent, so without extended statistics the planner multiplies two independent selectivities and badly underestimates funnel queries, which is what flips a hash join to a nested loop and turns 80 ms into minutes. Statistics on the partitioned PARENT were measured to have zero effect, so each partition needs its own object and a new partition is the moment this is forgotten.',
 NULL, 0, 0,
$q$
SELECT c.relname AS partition_name, c.reltuples::bigint AS estimated_rows
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
WHERE i.inhparent = '{{star}}.fct_deal_stage_event'::regclass
  AND c.reltuples > 0
  AND NOT EXISTS (
      SELECT 1 FROM pg_statistic_ext e
      JOIN pg_statistic_ext_data d ON d.stxoid = e.oid
      WHERE e.stxrelid = c.oid AND d.stxdndistinct IS NOT NULL
  )
$q$),

('ASSERT-WH-030', 'Unresolved dimension keys above the tolerated share', 'warehouse', 'reconciliation', 'warn',
 'Facts landing on the unknown member is the DESIGNED behaviour, and it is what makes warehouse totals reconcile to the raw feed instead of silently shrinking. But it is only acceptable in small volume: past a few percent, the broker and agency breakdowns are mostly a bucket called Unknown, and the dimension has stopped doing its job. This reports the share rather than forbidding it.',
 'D07 brokers absent from the directory and D08 property name variants', 0, 0,
$q$
WITH totals AS (
    SELECT count(*)::numeric AS all_rows,
           count(*) FILTER (WHERE broker_sk   = -1) AS unknown_broker,
           count(*) FILTER (WHERE property_sk = -1) AS unknown_property
    FROM {{star}}.fct_deal_stage_event
)
SELECT 'broker' AS dimension, unknown_broker AS unresolved_rows, all_rows::bigint,
       round(100.0 * unknown_broker / GREATEST(all_rows, 1), 3) AS pct_unresolved
FROM totals WHERE all_rows > 0 AND unknown_broker > 0.03 * all_rows
UNION ALL
SELECT 'property', unknown_property, all_rows::bigint,
       round(100.0 * unknown_property / GREATEST(all_rows, 1), 3)
FROM totals WHERE all_rows > 0 AND unknown_property > 0.03 * all_rows
$q$);

-- ---------------------------------------------------------------------
-- READER CONVENIENCES
-- ---------------------------------------------------------------------
-- The catalogue as a human readable contract. This is what gets pasted
-- into a handover document, so it lives in the database and cannot drift
-- away from the rules it describes.
CREATE OR REPLACE VIEW dq.v_assertion_catalogue AS
SELECT a.battery, a.quality_dimension, a.severity, a.assertion_code,
       a.assertion_name, a.expect_min, a.expect_max,
       a.seeded_defect, a.protects_against
FROM dq.assertion a
WHERE a.is_enabled
ORDER BY a.battery, a.assertion_code;
COMMENT ON VIEW dq.v_assertion_catalogue IS
'The assertion contract in reading order. Every row states what it protects against in business terms, because a rule nobody can justify is a rule that gets disabled.';

-- The latest verdict per assertion, which is what a monitor scrapes.
CREATE OR REPLACE VIEW dq.v_assertion_latest AS
SELECT r.battery, r.star_schema, r.started_at,
       res.assertion_code, a.assertion_name, res.quality_dimension,
       res.severity, res.status, res.offending_rows,
       res.expect_min, res.expect_max, res.duration_ms,
       res.error_text, res.offending_sample
FROM dq.assertion_result res
JOIN dq.assertion_run  r ON r.assertion_run_id = res.assertion_run_id
JOIN dq.assertion      a ON a.assertion_code   = res.assertion_code
WHERE r.assertion_run_id IN (
    -- One winner per (battery, star_schema): the most recent run of each.
    SELECT DISTINCT ON (battery, star_schema) assertion_run_id
    FROM dq.assertion_run
    ORDER BY battery, star_schema, started_at DESC
);
COMMENT ON VIEW dq.v_assertion_latest IS
'The most recent result for every assertion, per battery and per target schema. The shape a monitor or a Power BI page reads.';

\echo '07_quality.sql complete: dq.assertion catalogue, run log, result log, runner function and two reader views.'
