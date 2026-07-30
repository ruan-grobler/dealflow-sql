-- =====================================================================
-- 01_staging.sql
-- DealFlow warehouse: foundation, landing zone and typed staging layer.
--
-- WHAT THIS FILE OWNS
--   1. The reporting timezone pin, which must happen before any timestamp
--      is written, because valid_from in the SCD Type 2 dimensions is
--      derived from a date cast and a date cast is timezone dependent.
--   2. The seven namespaces and the determinism helper.
--   3. meta (run log, watermarks, partition registry).
--   4. dq (rules as data, results, rejects, quarantine, lineage).
--   5. raw (landing zone, every source column text, append only).
--   6. stg (typed, trimmed, renamed, still one row per surviving raw row).
--
-- BUILD SEMANTICS
--   01 and 02 are build-from-empty DDL: they DROP and recreate their
--   schemas so the warehouse is reproducible from nothing in one command.
--   03 and 04 are idempotent and safe to re-run against a built warehouse.
--   Run order is 01, 02, 03, 04.
--
-- WHY A LANDING ZONE AT ALL
--   raw is the replay point and the audit trail. Nothing is ever rejected,
--   updated or deleted here, so if a transform is found to be wrong six
--   months from now the whole warehouse can be rebuilt without going back
--   to the source systems. That property is worth one extra copy of the
--   data and it is the reason every source column is text: a cast that
--   fails must fail in stg where it can be diverted, not in raw where it
--   would reject the evidence.
-- =====================================================================

\set ON_ERROR_STOP on

-- ---------------------------------------------------------------------
-- STEP 0. Pin the reporting timezone IN THE DATABASE, not in a README.
--
-- MEASURED REASON: '2025-06-01'::date::timestamptz returns
-- 2025-06-01 00:00:00+00 under Etc/UTC and 2025-06-01 00:00:00+02 under
-- Africa/Johannesburg. The SCD Type 2 loader casts snapshot_date to
-- timestamptz to build valid_from, so an unpinned timezone makes the
-- dimension's validity windows machine dependent and the dataset
-- irreproducible. ALTER DATABASE persists it for every future session.
-- The SET makes it true for this session as well, because ALTER DATABASE
-- alone does not affect a connection that is already open.
-- ---------------------------------------------------------------------
-- current_database() rather than a literal name. .env, run.sh and all three
-- Python scripts read POSTGRES_DB from the environment, so hardcoding
-- "dealflow" here made this the single place a renamed database would break,
-- on the first statement of the first file, with an error that never
-- mentions .env.
DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone = %L',
                   current_database(), 'Africa/Johannesburg');
END $$;
SET timezone = 'Africa/Johannesburg';

DO $$
BEGIN
    IF current_setting('TimeZone') <> 'Africa/Johannesburg' THEN
        RAISE EXCEPTION 'Reporting timezone is %, expected Africa/Johannesburg. Aborting before any timestamp is written.',
            current_setting('TimeZone');
    END IF;
END $$;

-- btree_gist supplies the "=" operator class that lets the SCD Type 2
-- EXCLUDE constraint in 02 mix a text equality test with a range overlap
-- test inside one GiST index. Without it that constraint cannot be built.
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Ranks the real workload by total execution time, which is what block
-- 850_workload_ranking in sql/06_performance.sql reads to produce
-- plans/evidence_850_workload_ranking.txt. Already preloaded on this
-- container via shared_preload_libraries, because the counters have to be
-- collected from server start and no session can turn that on afterwards.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

DROP SCHEMA IF EXISTS raw  CASCADE;
DROP SCHEMA IF EXISTS stg  CASCADE;
DROP SCHEMA IF EXISTS int  CASCADE;
DROP SCHEMA IF EXISTS mart CASCADE;
DROP SCHEMA IF EXISTS dq   CASCADE;
DROP SCHEMA IF EXISTS meta CASCADE;
DROP SCHEMA IF EXISTS util CASCADE;

-- Six schemas hold data, one holds functions. A table's address states its
-- role in the pipeline, so nobody has to guess whether a table is a source
-- of truth or a working copy.
CREATE SCHEMA raw;    COMMENT ON SCHEMA raw  IS 'Landing zone. Every source column text. Append only, never rejected, never updated. The replay point.';
CREATE SCHEMA stg;    COMMENT ON SCHEMA stg  IS 'Typed, trimmed, renamed. One row per raw row that survived casting. No joins, no dedupe, no business logic.';
CREATE SCHEMA int;    COMMENT ON SCHEMA int  IS 'Intermediate. Conform, dedupe, sequence, resolve surrogate keys. Rebuilt per run.';
CREATE SCHEMA mart;   COMMENT ON SCHEMA mart IS 'The star. Conformed dimensions, two fact tables, materialized aggregates.';
CREATE SCHEMA dq;     COMMENT ON SCHEMA dq   IS 'Data quality: rules as data, results per run, rejects, quarantine, dedupe and restatement lineage.';
CREATE SCHEMA meta;   COMMENT ON SCHEMA meta IS 'Pipeline control: load run log, watermarks, partition registry.';
CREATE SCHEMA util;   COMMENT ON SCHEMA util IS 'Functions only: determinism helper, partition maker, statistics maker, DQ gate runner.';

-- =====================================================================
-- THE DETERMINISM CONTRACT
-- =====================================================================
-- The synthetic dataset must be byte identical on every machine and every
-- run, otherwise none of the measurements in the README can be reproduced.
--
-- setseed() DOES NOT ACHIEVE THIS. Measured on this container with
-- debug_parallel_query = on, the same setseed() plus random() query run
-- twice returned checksums 896e96ecb201de11365f539f60bac55c and
-- 7761a4e11696f15a8fd58b290be6dfa1, because every parallel worker seeds
-- its own generator. Any query over 250,000 rows goes parallel here.
--
-- fn_u derives a uniform draw from md5(seed || row key) instead. The draw
-- is a pure function of the row's own natural key, so it does not depend
-- on plan shape, worker count or row order. Measured: identical checksum
-- cffbd8ec1cbfa785245a14cfa1d78590 under a forced parallel plan and under
-- serial execution.
--
-- IMMUTABLE lets the planner fold it and inline it. PARALLEL SAFE lets it
-- run inside a worker, which is the whole point.
CREATE OR REPLACE FUNCTION util.fn_u(p_seed text, p_key text)
RETURNS double precision
LANGUAGE sql IMMUTABLE PARALLEL SAFE STRICT AS $$
    SELECT ('x' || substr(md5(p_seed || p_key), 1, 8))::bit(32)::bigint / 4294967296.0;
$$;
COMMENT ON FUNCTION util.fn_u(text, text) IS
'Deterministic uniform [0,1) from md5(seed || row key). Replaces random(), which is not reproducible under a parallel plan. Each seed string is an independent draw namespace.';

-- =====================================================================
-- ERROR TOLERANT CAST HELPERS
-- =====================================================================
-- WHY THEY EXIST: a row that cannot be cast must NOT abort a 3am load. It
-- must be diverted to a reject table with a reason code and the original
-- text, and the run must continue. PostgreSQL has no TRY_CAST, so an
-- exception handler is the mechanism.
--
-- THEY ARE MARKED PARALLEL UNSAFE, AND THAT WAS MEASURED THE HARD WAY.
-- Marked PARALLEL SAFE, the first version of the staging load failed with:
--   ERROR: cannot start subtransactions during a parallel operation
--   CONTEXT: PL/pgSQL function util.fn_try_ts(text) line 2 during
--            statement block entry
-- A plpgsql EXCEPTION block opens a subtransaction and a parallel worker
-- cannot open one, so an exception based TRY_CAST forces a SERIAL plan on
-- every query that mentions it. That is a real and rarely stated cost of
-- error tolerant loading.
--
-- WHICH IS WHY THE LOAD IN 03 DOES NOT USE THEM ON THE HOT PATH.
-- The two big feeds are classified with the regex predicates below, which
-- are IMMUTABLE and PARALLEL SAFE, and only the rows that pass are cast.
-- The try functions stay because they are the correct tool for a one off
-- repair or an interactive investigation, and because the reason they are
-- unsafe is worth stating rather than discovering twice.
CREATE OR REPLACE FUNCTION util.fn_try_ts(p_txt text)
RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE PARALLEL UNSAFE AS $fn$
BEGIN
    RETURN p_txt::timestamptz;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $fn$;
COMMENT ON FUNCTION util.fn_try_ts(text) IS
'TRY_CAST to timestamptz. PARALLEL UNSAFE because the exception block opens a subtransaction, which a parallel worker cannot do.';

CREATE OR REPLACE FUNCTION util.fn_try_numeric(p_txt text)
RETURNS numeric LANGUAGE plpgsql IMMUTABLE PARALLEL UNSAFE AS $fn$
BEGIN
    RETURN p_txt::numeric;
EXCEPTION WHEN others THEN
    RETURN NULL;
END $fn$;
COMMENT ON FUNCTION util.fn_try_numeric(text) IS
'TRY_CAST to numeric. PARALLEL UNSAFE for the same reason as fn_try_ts.';

-- ---------------------------------------------------------------------
-- THE PARALLEL SAFE CLASSIFIERS. These are what the load actually uses.
--
-- fn_is_real_date is not "does this look like a date". It answers "will
-- the cast succeed", which is a different and harder question, because
-- 2026-02-30 and 2025-13-04 both look like dates and neither exists. The
-- test therefore reassembles the date from its own parts and compares:
-- make_date would raise on an impossible day, so the arithmetic form is
-- used instead and the result is checked against the input.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_is_real_date(p_txt text)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT p_txt ~ '^\d{4}-\d{2}-\d{2}'
       AND substr(p_txt, 6, 2)::integer BETWEEN 1 AND 12
       AND substr(p_txt, 9, 2)::integer BETWEEN 1 AND
           EXTRACT(DAY FROM (make_date(substr(p_txt, 1, 4)::integer,
                                       substr(p_txt, 6, 2)::integer, 1)
                             + interval '1 month - 1 day'))::integer;
$fn$;
COMMENT ON FUNCTION util.fn_is_real_date(text) IS
'True when the leading yyyy-mm-dd of the text is a date that actually exists. Catches 2026-02-30 and 2025-13-04, which a regex alone accepts. IMMUTABLE PARALLEL SAFE, so the staging load stays parallel.';

CREATE OR REPLACE FUNCTION util.fn_is_numeric(p_txt text)
RETURNS boolean LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT p_txt ~ '^-?\d+(\.\d+)?$';
$fn$;
COMMENT ON FUNCTION util.fn_is_numeric(text) IS
'True when the text is a plain signed decimal. Deliberately strict: "R 42 mil" and "1,2 miljoen" must fail, because guessing at them would put a fabricated number in the warehouse.';

-- ---------------------------------------------------------------------
-- fn_norm_ws. The repair for keyboard noise.
--
-- The feeds send "  INDUSTRIAL", "industrial   " and " Retail  Centre "
-- for the same value, because a human retyped it. Left alone, each variant
-- opens its own Type 2 dimension version, so a 3 percent keying defect
-- would inflate the dimension and split every rollup. Trim, collapse
-- internal runs of whitespace, and let the caller compare case
-- insensitively against a canonical list.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_norm_ws(p_txt text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT nullif(btrim(regexp_replace(p_txt, '\s+', ' ', 'g')), '');
$fn$;
COMMENT ON FUNCTION util.fn_norm_ws(text) IS
'Trim, collapse internal whitespace, empty string to NULL. The minimum repair that makes two keyboard variants of one value compare equal.';

-- ---------------------------------------------------------------------
-- fn_norm_name. Fuzzy matching WITHOUT a fuzzy match.
--
-- 0.5 percent of submissions arrive with no property code, only a building
-- name a broker typed, often with a legal suffix bolted on ("Erf 4821
-- Sandton (Pty) Ltd"). The register holds "Erf 4821 Sandton".
--
-- WHY NOT trigram similarity: pg_trgm on 12,000 register names is a real
-- option, but similarity() returns a SCORE and a score needs a threshold,
-- and a threshold is a number nobody can defend in a review. Deterministic
-- normalisation either matches or it does not, the same way on every run,
-- and the unmatched remainder lands on the unknown member where DQ-002
-- counts it. A reviewer can audit that. A 0.42 cutoff, they cannot.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_norm_name(p_txt text)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $fn$
    SELECT nullif(btrim(regexp_replace(
               regexp_replace(
                   regexp_replace(lower(p_txt),
                       '\s*\(?\b(pty|proprietary|ltd|limited|inc|cc|trust|holdings)\b\)?\.?', '', 'g'),
                   '[^a-z0-9 ]', ' ', 'g'),
               '\s+', ' ', 'g')), '');
$fn$;
COMMENT ON FUNCTION util.fn_norm_name(text) IS
'Lower case, strip legal suffixes and punctuation, collapse whitespace. Deterministic name conform, deliberately chosen over trigram similarity because a normalisation is auditable and a similarity threshold is not.';

-- =====================================================================
-- meta. PIPELINE CONTROL
-- =====================================================================

CREATE TABLE meta.load_run (
    run_id              bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    load_mode           text        NOT NULL,
    status              text        NOT NULL DEFAULT 'RUNNING',
    started_at          timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at         timestamptz,
    duration_ms         numeric(12,1),
    rows_raw_available  bigint,
    rows_staged         bigint,
    rows_rejected       bigint,
    rows_quarantined    bigint,
    rows_deduped_out    bigint,
    rows_dim_versioned  bigint,
    rows_fact_inserted  bigint,
    rows_fact_updated   bigint,
    rows_pipeline_upsert bigint,
    dq_error_failures   integer,
    dq_warn_failures    integer,
    notes               text,
    CONSTRAINT ck_load_run_status CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    CONSTRAINT ck_load_run_mode   CHECK (load_mode IN ('INITIAL', 'INCREMENTAL'))
);
COMMENT ON TABLE meta.load_run IS
'One row per pipeline run. Every audit column elsewhere points at run_id, so any row in the warehouse can be traced to the run that produced it.';

CREATE TABLE meta.load_watermark (
    stream_nk    text        NOT NULL PRIMARY KEY,
    stream_kind  text        NOT NULL,
    watermark_ts timestamptz NOT NULL,
    watermark_key text,
    last_run_id  bigint      REFERENCES meta.load_run (run_id),
    updated_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_watermark_kind CHECK (stream_kind IN ('INGEST', 'SCD_APPLY'))
);
COMMENT ON TABLE meta.load_watermark IS
'One row per stream. Four INGEST rows hold the raw.ingested_at high water mark that drives the incremental staging MERGE. Two SCD_APPLY rows hold the last dimension snapshot already folded into a Type 2 dimension. Advanced only after the final DQ gate passes, so a failed run reprocesses instead of skipping data.';

-- -infinity is the "nothing loaded yet" sentinel. It makes the first run a
-- full load through exactly the same code path as every later run, so
-- there is no separate initial load script to keep in step.
INSERT INTO meta.load_watermark (stream_nk, stream_kind, watermark_ts) VALUES
    ('raw.deal_submission',  'INGEST',    '-infinity'),
    ('raw.stage_event',      'INGEST',    '-infinity'),
    ('raw.broker_directory', 'INGEST',    '-infinity'),
    ('raw.property_register','INGEST',    '-infinity'),
    ('mart.dim_broker',      'SCD_APPLY', '-infinity'),
    ('mart.dim_property',    'SCD_APPLY', '-infinity');

CREATE TABLE meta.partition_registry (
    partition_name    text NOT NULL PRIMARY KEY,
    parent_table      text NOT NULL,
    range_start       date NOT NULL,
    range_end         date,
    is_default        boolean NOT NULL DEFAULT false,
    created_at        timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_by_run_id bigint,
    stats_object      text,
    row_count         bigint,
    size_bytes        bigint,
    detached_at       timestamptz
);
COMMENT ON TABLE meta.partition_registry IS
'One row per fact partition. Gives the retention job an audit trail and lets a reader see which months exist without reading pg_inherits.';

-- =====================================================================
-- dq. QUALITY RULES AS DATA
-- =====================================================================
-- Rules live in a table, not in code, so adding a rule is an INSERT and
-- the rule set is queryable ("show me every error rule that guards the
-- star"). Each rule's sql_expression returns ONE numeric observation and
-- the verdict is observation BETWEEN threshold_min AND threshold_max.
-- One uniform shape covers reconciliation rules (band 0 to 0), count
-- ceilings (0 to n) and "must be non-zero but small" bands alike.

CREATE TABLE dq.check_definition (
    check_code     text    NOT NULL PRIMARY KEY,
    check_name     text    NOT NULL,
    layer          text    NOT NULL,
    severity       text    NOT NULL,
    observation    text    NOT NULL,
    sql_expression text    NOT NULL,
    threshold_min  numeric,
    threshold_max  numeric,
    seeded_defect  text    NOT NULL,
    is_enabled     boolean NOT NULL DEFAULT true,
    CONSTRAINT ck_check_layer    CHECK (layer IN ('stg', 'int', 'mart')),
    CONSTRAINT ck_check_severity CHECK (severity IN ('error', 'warn', 'info'))
);
COMMENT ON TABLE dq.check_definition IS
'One row per data quality rule. severity error aborts the run at its gate, warn and info are recorded and trended. seeded_defect names the generator defect the rule exists to catch, so no rule is decorative and no defect is unguarded.';
COMMENT ON COLUMN dq.check_definition.sql_expression IS
'SQL returning exactly one numeric observation. Executed by util.fn_run_dq_gate via EXECUTE, which is why only this project writes rows here.';

-- ---------------------------------------------------------------------
-- THE RULE CATALOGUE. 12 rules, and every one of them names the generator
-- defect it exists to catch. Two rules deserve their reasoning up front.
--
-- DQ-004 and DQ-012 are the two gates that a naive version of this project
-- gets wrong. The generator deliberately seeds events for deals that were
-- never successfully submitted, and event dates outside the partition
-- range. If DQ-004 asserted "every fact deal has a pipeline row" before
-- those orphans were quarantined, and DQ-012 asserted "the DEFAULT
-- partition is empty", then the pipeline would stop every single run on
-- its own demo data. So the orphans are quarantined in int BEFORE DQ-004
-- runs, and DQ-012 is a warn rule with an expected NON-ZERO band, because
-- a partition that is deliberately fed cannot also be asserted empty.
--
-- Gates that are DESIGNED to record a non-zero observation on this data:
--   DQ-010 (duplicate rate), DQ-011 (value quality), DQ-012 (default
--   partition depth). All three are warn and all three must stay in band.
-- Gates that must NEVER fail: every error rule below.
-- ---------------------------------------------------------------------
INSERT INTO dq.check_definition
    (check_code, check_name, layer, severity, observation, threshold_min, threshold_max, seeded_defect, sql_expression) VALUES

('DQ-001', 'raw to stg reconciles', 'stg', 'error',
 'Absolute difference between raw rows offered and (staged + rejected + quarantined).', 0, 0,
 'Defects 4 and 5: unparseable value strings and impossible dates.',
$q$
-- Every raw row must end up in exactly one place: staged, rejected or
-- quarantined. This is the rule that proves error tolerant loading did not
-- quietly lose anything, which is the whole risk of diverting bad rows.
WITH offered AS (
    SELECT (SELECT count(*) FROM raw.deal_submission) AS submissions,
           (SELECT count(*) FROM raw.stage_event)     AS events
), landed AS (
    SELECT (SELECT count(*) FROM stg.deal_submission)                                     AS staged_sub,
           (SELECT count(*) FROM dq.reject_deal_submission)                               AS rejected_sub,
           (SELECT count(*) FROM stg.stage_event)                                         AS staged_evt,
           (SELECT count(*) FROM dq.quarantine_stage_event WHERE detected_layer = 'stg')   AS rejected_evt
)
SELECT abs(o.submissions - (l.staged_sub + l.rejected_sub))
     + abs(o.events      - (l.staged_evt + l.rejected_evt))
FROM offered o CROSS JOIN landed l
$q$),

('DQ-002', 'conform completeness', 'int', 'error',
 'Rows leaving the conform layer with a natural key or a classification still NULL.', 0, 0,
 'Defect 6 (broker email case and whitespace variants) and defect 13 (properties with a blank sector).',
$q$
-- After conforming, a deal must know its broker and its property and the
-- property must have a sector. Unknown is an allowed answer (that is what
-- the unknown member is for), NULL is not, because NULL silently drops
-- rows out of every inner join downstream.
SELECT count(*) FROM int.deal_deduped
WHERE broker_nk IS NULL OR property_nk IS NULL OR sector IS NULL
$q$),

('DQ-003', 'dedupe grain holds', 'int', 'error',
 'Content fingerprints with more than one surviving deal.', 0, 0,
 'Defect 1: duplicate resubmissions under a fresh message id with a jittered value.',
$q$
SELECT count(*) FROM (
    SELECT deal_fingerprint FROM int.deal_deduped GROUP BY deal_fingerprint HAVING count(*) > 1
) x
$q$),

('DQ-004', 'no orphan deals in the star', 'mart', 'error',
 'Distinct deal_nk in the event fact with no row in the accumulating snapshot.', 0, 0,
 'Defect 12: events for a deal reference that was never successfully submitted. Quarantined in int, so this rule verifies the quarantine held.',
$q$
SELECT count(*) FROM (
    SELECT DISTINCT f.deal_nk
    FROM mart.fct_deal_stage_event f
    WHERE NOT EXISTS (SELECT 1 FROM mart.fct_deal_pipeline p WHERE p.deal_nk = f.deal_nk)
) x
$q$),

('DQ-005', 'point in time resolution is correct', 'mart', 'error',
 'Fact rows whose broker or property surrogate key points at a dimension version not valid at event_ts.', 0, 0,
 'Defect 7 (brokers absent from the directory) and defect 8 (property name variants). Both land on the unknown member, and this rule proves every other row time aligned correctly.',
$q$
-- The foreign keys already guarantee the key EXISTS. Only this rule
-- guarantees the key was the version in force when the event happened,
-- which is the entire point of a Type 2 dimension and the one thing a
-- foreign key cannot check.
SELECT (
    SELECT count(*) FROM mart.fct_deal_stage_event f
    JOIN mart.dim_broker b ON b.broker_sk = f.broker_sk
    WHERE b.broker_sk <> -1
      AND NOT (f.event_ts >= b.valid_from AND f.event_ts < b.valid_to)
) + (
    SELECT count(*) FROM mart.fct_deal_stage_event f
    JOIN mart.dim_property p ON p.property_sk = f.property_sk
    WHERE p.property_sk <> -1
      AND NOT (f.event_ts >= p.valid_from AND f.event_ts < p.valid_to)
)
$q$),

('DQ-006', 'one current version per natural key', 'mart', 'error',
 'Natural keys with more than one current row across both Type 2 dimensions.', 0, 0,
 'Defect 14: the same broker listed twice in one directory snapshot, which would open two versions at once if the loader did not dedupe the source.',
$q$
-- The EXCLUDE constraint on each dimension is what MAKES this impossible
-- (two open versions both end at infinity, so they always overlap). This
-- rule is not a second enforcement mechanism, it is the trend line: it
-- puts a number in dq.check_result every run so the invariant is visibly
-- holding rather than merely assumed.
SELECT (
    SELECT count(*) FROM (SELECT broker_nk FROM mart.dim_broker WHERE is_current
                          GROUP BY broker_nk HAVING count(*) > 1) a
) + (
    SELECT count(*) FROM (SELECT property_nk FROM mart.dim_property WHERE is_current
                          GROUP BY property_nk HAVING count(*) > 1) b
)
$q$),

('DQ-007', 'fact event stream integrity', 'mart', 'error',
 'Fact events that precede their deal receipt, plus fact events that move out of a terminal stage.', 0, 0,
 'Defect 10 (events arriving out of order) and defect 11 (physically impossible transitions).',
$q$
SELECT (
    SELECT count(*) FROM mart.fct_deal_stage_event f
    JOIN mart.fct_deal_pipeline p ON p.deal_nk = f.deal_nk
    WHERE f.event_ts < p.received_ts
) + (
    SELECT count(*) FROM mart.fct_deal_stage_event f
    JOIN mart.dim_stage s ON s.stage_sk = f.from_stage_sk
    WHERE s.is_terminal
)
$q$),

('DQ-008', 'every populated partition has extended statistics', 'mart', 'error',
 'Non-empty partitions of the event fact with no populated extended statistics object.', 0, 0,
 'No generator defect. This rule guards an internal invariant: extended statistics on the partitioned PARENT were measured to have zero effect on estimates, so a partition without its own statistics object silently gets bad row estimates.',
$q$
SELECT count(*)
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
WHERE i.inhparent = 'mart.fct_deal_stage_event'::regclass
  AND c.reltuples > 0
  AND NOT EXISTS (
      SELECT 1 FROM pg_statistic_ext e
      JOIN pg_statistic_ext_data d ON d.stxoid = e.oid
      WHERE e.stxrelid = c.oid AND d.stxdndistinct IS NOT NULL
  )
$q$),

('DQ-009', 'no negative money reached the star', 'mart', 'error',
 'Fact rows with a negative deal value.', 0, 0,
 'Defect 9: negative values from a credit note sign typo. Repaired with abs() in staging, so this rule proves the repair held.',
$q$
SELECT (SELECT count(*) FROM mart.fct_deal_stage_event WHERE deal_value_zar < 0)
     + (SELECT count(*) FROM mart.fct_deal_pipeline    WHERE deal_value_zar < 0)
$q$),

('DQ-010', 'duplicate submission rate in band', 'int', 'warn',
 'Suppressed duplicates as a share of staged submissions.', 0.020, 0.070,
 'Defect 1: duplicate resubmissions, seeded at 5 percent of deals. Some duplicates fail casting first, so the rate observed at the dedupe step is legitimately lower than the seeded rate, and the band allows for that.',
$q$
SELECT round(
    (SELECT count(*) FROM dq.duplicate_submission)::numeric
  / GREATEST((SELECT count(*) FROM stg.deal_submission), 1), 5)
$q$),

('DQ-011', 'submission value quality in band', 'int', 'warn',
 'Deals with no value plus deals with an implausibly small value.', 0, 8000,
 'Defect 2 (value missing entirely) and defect 3 (value entered in thousands rather than units).',
$q$
SELECT count(*) FROM int.deal_deduped
WHERE deal_value_zar IS NULL OR deal_value_zar < 100000
$q$),

('DQ-012', 'default partition depth in band', 'mart', 'warn',
 'Rows in the DEFAULT partition of the event fact.', 1, 8000,
 'Defect 15: event dates outside the declared partition range. The band starts at 1 because this data is deliberately seeded with them; a zero would mean the generator stopped producing the defect and the rule stopped being tested.',
$q$
SELECT count(*) FROM mart.fct_deal_stage_event_default
$q$);

CREATE TABLE dq.check_result (
    check_result_sk bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id          bigint  NOT NULL REFERENCES meta.load_run (run_id),
    check_code      text    NOT NULL REFERENCES dq.check_definition (check_code),
    layer           text    NOT NULL,
    severity        text    NOT NULL,
    observed_value  numeric,
    threshold_min   numeric,
    threshold_max   numeric,
    is_pass         boolean NOT NULL,
    duration_ms     numeric(12,1),
    checked_at      timestamptz NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE dq.check_result IS
'One row per rule per run. Append only, so data quality is a trend line and not a one-off assertion. The thresholds are copied in so a later threshold change does not rewrite history.';
CREATE INDEX ix_dq_check_result_run ON dq.check_result (run_id, check_code);

CREATE TABLE dq.reject_deal_submission (
    reject_sk         bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id            bigint NOT NULL REFERENCES meta.load_run (run_id),
    reason_code       text   NOT NULL,
    source_file       text   NOT NULL,
    raw_line_no       bigint NOT NULL,
    source_message_id text,
    deal_ref          text,
    offending_value   text,
    raw_payload       jsonb  NOT NULL,
    rejected_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_reject_submission UNIQUE (source_file, raw_line_no)
);
COMMENT ON TABLE dq.reject_deal_submission IS
'One row per raw submission that failed a hard cast, with the reason code and the original text. Rows are diverted, never dropped, and the run continues: one unparseable value must not cost the other 262,499 rows their load.';

CREATE TABLE dq.quarantine_stage_event (
    quarantine_sk bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id        bigint NOT NULL REFERENCES meta.load_run (run_id),
    reason_code   text   NOT NULL,
    event_nk      text,
    deal_nk       text,
    stage_nk      text,
    event_ts_text text,
    detected_layer text  NOT NULL,
    source_file   text   NOT NULL,
    raw_line_no   bigint NOT NULL,
    raw_payload   jsonb,
    quarantined_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    -- THE UNIT OF QUARANTINE IS A RAW ROW, NOT AN EVENT, and that is a
    -- correctness fix rather than a preference. UNIQUE (event_nk) was tried
    -- first and it cannot express the redelivery defect: the source resends a
    -- payload verbatim, so TWO raw rows carry ONE event id, one of them is
    -- staged and one is held back, and a key on event_nk has no room for the
    -- second. DQ-001 reconciles raw against staged plus quarantined, so the
    -- missing row showed up as an off by one in the reconciliation. Keyed on
    -- the raw address the arithmetic is exact whatever the defect.
    CONSTRAINT uq_quarantine_raw_row UNIQUE (source_file, raw_line_no)
);
COMMENT ON TABLE dq.quarantine_stage_event IS
'One row per stage event held back from the fact, with the reason code and the layer that caught it. Reason codes: UNKNOWN_STAGE_CODE, UNPARSEABLE_EVENT_TS and REDELIVERED_PAYLOAD (stg), IMPOSSIBLE_TRANSITION and ORPHAN_DEAL (int).';
COMMENT ON COLUMN dq.quarantine_stage_event.detected_layer IS
'The layer that caught it, and it is load bearing for DQ-001: that rule reconciles raw against staged plus quarantined AT stg only, so an event held back later in int must not be counted twice.';

CREATE TABLE dq.duplicate_submission (
    duplicate_sk        bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id              bigint NOT NULL REFERENCES meta.load_run (run_id),
    source_message_nk   text   NOT NULL,
    deal_nk             text   NOT NULL,
    duplicate_of_deal_nk text  NOT NULL,
    deal_fingerprint    text   NOT NULL,
    days_after_original numeric(8,2),
    -- numeric(12,4) and not numeric(8,4). MEASURED: a resubmission of a row
    -- whose value was keyed in thousands yields a delta of about 99,900
    -- percent, and the narrower column raised "numeric field overflow" on the
    -- real data. The column is evidence, so it has to be able to hold the
    -- evidence rather than force the loader to discard the extreme cases that
    -- are the most interesting ones.
    value_delta_pct     numeric(12,4),
    detected_at         timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT uq_duplicate_submission UNIQUE (source_message_nk)
);
COMMENT ON TABLE dq.duplicate_submission IS
'One row per submission suppressed by content-fingerprint dedupe, carrying duplicate_of_deal_nk. Dedupe never deletes: keeping the lineage is what makes "which brokers resubmit most" answerable and the duplicate rate auditable.';

CREATE TABLE dq.restatement_log (
    restatement_sk bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    run_id         bigint NOT NULL REFERENCES meta.load_run (run_id),
    deal_nk        text   NOT NULL,
    attribute_name text   NOT NULL,
    old_value      text,
    new_value      text,
    restated_at    timestamptz NOT NULL DEFAULT clock_timestamp()
);
COMMENT ON TABLE dq.restatement_log IS
'One row per attribute overwritten in place on the accumulating snapshot fact. The accumulating snapshot is current truth and the event fact is the immutable audit trail, so this table is the only record that a value used to be something else.';
CREATE INDEX ix_dq_restatement_deal ON dq.restatement_log (deal_nk);

-- =====================================================================
-- raw. LANDING ZONE
-- =====================================================================
-- THE COLUMN CONTRACT IS THE SOURCE'S, NOT THE WAREHOUSE'S.
--   Every column below is named exactly what the feed calls it and typed
--   text, because raw's job is to be a faithful copy of what arrived. The
--   generator (generate_data.py, which stands in for four source systems)
--   COPYs into these tables by column name, so this DDL and RAW_COLUMNS in
--   that file are one contract in two places. Renaming a column here
--   without renaming it there fails the COPY loudly on the next run, which
--   is the behaviour we want from a contract.
--
-- WHY EVERY SOURCE COLUMN IS text
--   A cast that fails must fail in stg, where the row can be diverted to a
--   reject table with a reason code. If it failed here it would reject the
--   evidence, and the evidence is the only thing that makes a replay
--   possible six months from now.
--
-- (source_file, raw_line_no) IS THE ADDRESS OF A RAW ROW.
--   It is what dq.reject_deal_submission and dq.quarantine_stage_event
--   point back at, so a rejected row can always be read in its original
--   form. A surrogate key here would be useless for that: it would only
--   identify the copy, not the line in the file the supplier sent.

CREATE TABLE raw.deal_submission (
    -- Source columns. All text, all as named by the feed.
    source_message_nk text,
    deal_nk           text,
    broker_nk         text,
    broker_name       text,
    broker_email      text,
    property_nk       text,
    property_name     text,
    sector_hint       text,
    deal_value_zar    text,
    currency_code     text,
    received_ts       text,
    source_channel    text,
    is_off_market     text,
    submission_notes  text,
    -- Ingest metadata. The only thing the landing zone asserts, because it
    -- is the only thing the warehouse knows for certain about a row it has
    -- not yet parsed.
    source_batch_id   text        NOT NULL,
    ingested_at       timestamptz NOT NULL,
    source_file       text        NOT NULL,
    raw_line_no       bigint      NOT NULL,
    payload_hash      text        NOT NULL
);
COMMENT ON TABLE raw.deal_submission IS
'One row per inbound broker deal submission exactly as received, including duplicate resubmissions, unparseable values and impossible dates. Append only, never rejected, never updated: this is the replay point.';
COMMENT ON COLUMN raw.deal_submission.source_batch_id IS
'The FEED''S batch tag, for example ING-SUB-2023-04. Deliberately not called run_id: meta.load_run.run_id is the WAREHOUSE''S run and is a bigint. One row therefore carries both, which is what lets a bad row be blamed on either the supplier batch or the load that read it.';
COMMENT ON COLUMN raw.deal_submission.payload_hash IS
'Hash of the source columns, supplied by the feed. Lets a replay prove it read the same bytes, and makes an accidental double ingest of one file detectable rather than merely suspected.';
COMMENT ON COLUMN raw.deal_submission.deal_value_zar IS
'text, and that is the point. About 0.6 percent of these are strings like "R 42 mil" or "TBC". A numeric column here would abort the load; text lets stg divert them with a reason code.';

CREATE TABLE raw.stage_event (
    source_event_nk text,
    deal_nk         text,
    stage_code      text,
    event_ts        text,
    actor_broker_nk text,
    stage_value_zar text,
    event_notes     text,
    source_batch_id text        NOT NULL,
    ingested_at     timestamptz NOT NULL,
    source_file     text        NOT NULL,
    raw_line_no     bigint      NOT NULL,
    payload_hash    text        NOT NULL
);
COMMENT ON TABLE raw.stage_event IS
'One row per deal stage transition payload as received from the pipeline system, including verbatim redeliveries, out of order arrivals, unknown stage codes, dates outside the partition range, and events for deal references that were never successfully submitted.';

CREATE TABLE raw.broker_directory (
    snapshot_date               text,
    broker_nk                   text,
    full_name                   text,
    broker_email                text,
    broker_phone                text,
    agency_code                 text,
    agency_name                 text,
    agency_tier                 text,
    agency_head_office_province text,
    agency_is_national          text,
    agency_headcount_band       text,
    region                      text,
    broker_tier                 text,
    is_active                   text,
    source_batch_id             text        NOT NULL,
    ingested_at                 timestamptz NOT NULL,
    source_file                 text        NOT NULL,
    raw_line_no                 bigint      NOT NULL,
    payload_hash                text        NOT NULL
);
COMMENT ON TABLE raw.broker_directory IS
'One row per broker per monthly FULL snapshot of the broker directory. A full snapshot source, so Type 2 change detection against it is a hash compare rather than a diff feed. The agency attributes ride along on the broker row, which is how a real HR export behaves.';

CREATE TABLE raw.property_register (
    property_nk         text,
    register_event_type text,
    effective_date      text,
    property_name       text,
    suburb              text,
    metro               text,
    province            text,
    sector              text,
    sub_sector          text,
    building_grade      text,
    gla_sqm             text,
    gla_sqm_band        text,
    source_batch_id     text        NOT NULL,
    ingested_at         timestamptz NOT NULL,
    source_file         text        NOT NULL,
    raw_line_no         bigint      NOT NULL,
    payload_hash        text        NOT NULL
);
COMMENT ON TABLE raw.property_register IS
'One row per property register event: an initial full load followed by a change only DELTA feed. Deliberately a different source shape from the broker directory, so the Type 2 loader is proved against both a full snapshot and a delta.';
COMMENT ON COLUMN raw.property_register.register_event_type IS
'INITIAL or RECLASSIFY. On a delta feed the source tells you what kind of change it is, and the loader must still not trust it: a RECLASSIFY that changed nothing tracked must not open a version.';

-- ---------------------------------------------------------------------
-- INDEXES ON raw. BRIN, AND ONLY BRIN.
--
-- QUERY SERVED: the incremental staging load in 03, whose only predicate
-- on raw is "ingested_at > watermark". Nothing else reads raw.
--
-- WHY BRIN AND NOT BTREE: raw is one large unpartitioned heap written in
-- ingest order, so ingested_at is almost perfectly correlated with
-- physical position. That is the exact condition BRIN is built for: it
-- stores one min/max pair per block range instead of one entry per row.
-- MEASURED ON THIS TABLE, WHICH IS NOT THE SAME AS THE LAB MIRROR, AND THE
-- DIFFERENCE IS WORTH KNOWING. BRIN is 24 kB here. The equivalent btree is
-- an order of magnitude smaller than the lab mirror's, because ingested_at
-- on THIS table is a real batch stamp: a whole file arrives and every row
-- in it carries the same timestamp, so there are 79 distinct values across
-- 1.2 million rows and btree deduplication collapses them into posting
-- lists. The lab mirror fills ingested_at from the event timestamp, making
-- it near unique, which is the case where a btree is largest. Both
-- measurements, both ratios and the reason for the gap are in PERFORMANCE.md
-- case 5, which is the one place they live. Evidence for the planner
-- actually choosing the BRIN (Bitmap Index Scan on ix_raw_ingested_brin) is
-- in plans/evidence_810_brin_on_raw.txt.
--
-- WHY THE DEFAULT pages_per_range: 128 already produced 24 kB. A smaller
-- range would only be justified by a measurement showing the summary was
-- too coarse, and there is none, so the default stands rather than an
-- unexplained magic number.
--
-- WHY THERE IS NO BRIN ON THE FACT: see the note in 02. It was built and
-- measured there, the planner DID choose it and it DID help, and it was
-- deleted anyway because rewriting the query onto the partition key beat it
-- and cost nothing.
-- ---------------------------------------------------------------------
CREATE INDEX ix_raw_deal_submission_brin   ON raw.deal_submission   USING brin (ingested_at);
CREATE INDEX ix_raw_stage_event_brin       ON raw.stage_event       USING brin (ingested_at);
CREATE INDEX ix_raw_broker_directory_brin  ON raw.broker_directory  USING brin (ingested_at);
CREATE INDEX ix_raw_property_register_brin ON raw.property_register USING brin (ingested_at);

-- =====================================================================
-- stg. TYPED STAGING
-- =====================================================================
-- One row per raw row that survived casting. Casting, trimming, case
-- folding and renaming only. No joins across sources, no dedupe, no
-- business logic: those belong in int, where they can be inspected
-- separately from the question "did this row parse".
--
-- Every stg table is keyed on its source key so the staging load can be a
-- MERGE, which is what makes a re-run of 03 a no-op instead of a double
-- insert.

CREATE TABLE stg.deal_submission (
    source_message_nk  text        NOT NULL PRIMARY KEY,
    deal_nk            text        NOT NULL,
    broker_nk          text,
    broker_email_norm  text,
    property_nk        text,
    property_name      text        NOT NULL,
    property_name_norm text        NOT NULL,
    sector_hint        text,
    deal_value_zar     numeric(18,2),
    received_ts        timestamptz NOT NULL,
    received_date      date        NOT NULL,
    source_channel     text        NOT NULL,
    is_off_market      boolean     NOT NULL,
    submission_quality text        NOT NULL,
    value_was_repaired boolean     NOT NULL,
    source_file        text        NOT NULL,
    raw_line_no        bigint      NOT NULL,
    source_batch_id    text        NOT NULL,
    ingested_at        timestamptz NOT NULL,
    loaded_by_run_id   bigint      NOT NULL,
    CONSTRAINT ck_stg_submission_quality CHECK (submission_quality IN ('clean', 'repaired', 'suspect'))
);
COMMENT ON TABLE stg.deal_submission IS
'One row per raw deal submission that survived type casting. Still one row per raw row: duplicate resubmissions are present here and are only suppressed in int, because "did this row parse" and "is this row a resubmission of another" are different questions and mixing them makes both unauditable.';
COMMENT ON COLUMN stg.deal_submission.broker_email_norm IS
'Email lower cased with whitespace stripped. The normalisation IS the repair for the case and whitespace defect; resolving it to a broker happens in int, because that needs the directory.';
COMMENT ON COLUMN stg.deal_submission.property_nk IS
'NULL is legal and is a seeded defect: about 0.5 percent of submissions name the building instead of giving its code. int matches those on property_name_norm, which is why that column is NOT NULL here.';
COMMENT ON COLUMN stg.deal_submission.submission_quality IS
'clean, repaired (a value or an identifier had to be corrected on the way in) or suspect (value missing or implausible). Becomes one third of the junk dimension key, so a report can exclude suspect submissions without re-deriving what suspect means.';
COMMENT ON COLUMN stg.deal_submission.value_was_repaired IS
'true when a negative value was made positive or a value keyed in thousands was scaled. Kept as its own boolean rather than inferred from submission_quality, because the repair and the grade are separately useful.';

CREATE TABLE stg.stage_event (
    event_nk         text        NOT NULL PRIMARY KEY,
    deal_nk          text        NOT NULL,
    stage_nk         text        NOT NULL,
    to_stage_sk      integer     NOT NULL,
    event_ts         timestamptz NOT NULL,
    actor_broker_nk  text,
    stage_value_zar  numeric(18,2),
    source_file      text        NOT NULL,
    raw_line_no      bigint      NOT NULL,
    source_batch_id  text        NOT NULL,
    ingested_at      timestamptz NOT NULL,
    loaded_by_run_id bigint      NOT NULL
);
COMMENT ON TABLE stg.stage_event IS
'One row per raw stage event that survived casting and stage code lookup. Keyed on event_nk, so a verbatim redelivery collapses here and is logged to dq.quarantine_stage_event to keep the row count reconciliation exact.';
COMMENT ON COLUMN stg.stage_event.raw_line_no IS
'Kept as the arrival order tiebreaker. The feed is not guaranteed to arrive in event order, and two events on one timestamp must sequence identically on every run or every dwell measure moves.';
-- QUERY SERVED: the int layer rebuilds sequencing per DEAL (a window
-- function needs the whole partition), so it reads every event for a set
-- of deals. deal_nk leads, event_ts second, which is also the exact
-- ORDER BY of the LAG that derives from_stage and dwell, so the window can
-- be fed from an ordered index scan instead of a sort.
CREATE INDEX ix_stg_stage_event_deal ON stg.stage_event (deal_nk, event_ts, raw_line_no);

CREATE TABLE stg.broker_directory (
    snapshot_date               date        NOT NULL,
    broker_nk                   text        NOT NULL,
    full_name                   text        NOT NULL,
    email_norm                  text,
    phone                       text,
    agency_code                 text        NOT NULL,
    agency_name                 text        NOT NULL,
    agency_tier                 text        NOT NULL,
    agency_head_office_province text        NOT NULL,
    agency_is_national          boolean     NOT NULL,
    agency_headcount_band       text        NOT NULL,
    region                      text        NOT NULL,
    broker_tier                 text        NOT NULL,
    is_active                   boolean     NOT NULL,
    source_file                 text        NOT NULL,
    raw_line_no                 bigint      NOT NULL,
    source_batch_id             text        NOT NULL,
    ingested_at                 timestamptz NOT NULL,
    loaded_by_run_id            bigint      NOT NULL,
    PRIMARY KEY (snapshot_date, broker_nk, raw_line_no)
);
COMMENT ON TABLE stg.broker_directory IS
'One row per broker per monthly snapshot, typed. raw_line_no is part of the key because a snapshot legitimately contains the same broker twice (a real export defect, seeded 12 times). The duplicate is resolved in int, not hidden here.';
COMMENT ON COLUMN stg.broker_directory.region IS
'Case and whitespace repaired against the canonical list. The feed sends "  COASTAL WEST" and "Coastal  West " for the same region, and an unrepaired variant would open a spurious Type 2 version on every snapshot it appeared in.';

CREATE TABLE stg.property_register (
    effective_date      date        NOT NULL,
    property_nk         text        NOT NULL,
    register_event_type text        NOT NULL,
    property_name       text        NOT NULL,
    property_name_norm  text        NOT NULL,
    suburb              text        NOT NULL,
    metro               text        NOT NULL,
    province            text        NOT NULL,
    sector              text        NOT NULL,
    sub_sector          text        NOT NULL,
    building_grade      text        NOT NULL,
    gla_sqm             integer,
    gla_sqm_band        text        NOT NULL,
    source_file         text        NOT NULL,
    raw_line_no         bigint      NOT NULL,
    source_batch_id     text        NOT NULL,
    ingested_at         timestamptz NOT NULL,
    loaded_by_run_id    bigint      NOT NULL,
    PRIMARY KEY (effective_date, property_nk, raw_line_no)
);
COMMENT ON TABLE stg.property_register IS
'One row per property register event, typed. sector is NOT NULL here because the conform step replaces a blank sector with Unknown: a missing classification is a KNOWN state, not an absent one, and NULL would silently drop the property out of every sector rollup.';
COMMENT ON COLUMN stg.property_register.property_name_norm IS
'Lower cased, legal suffixes and punctuation stripped, whitespace collapsed. This is the key that name only deal submissions are matched on, because brokers type the building name freehand.';

-- QUERY SERVED: the property name conform join in int, which matches a
-- submitted freehand building name to the register's normalised name and
-- needs the version in force at the time. Composite because the lookup is
-- always "this name, as at this date", never one without the other.
CREATE INDEX ix_stg_property_name ON stg.property_register (property_name_norm, effective_date);

\echo '01_staging.sql complete: 7 schemas, 6 util functions, 3 meta tables, 6 dq tables, 4 raw tables, 4 stg tables.'
