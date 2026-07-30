-- =====================================================================
-- 02_warehouse.sql
-- DealFlow warehouse: the dimensional model.
--
-- WHAT THIS FILE OWNS
--   1. util functions that build and maintain physical structure:
--      the partition maker, the per-partition statistics maker and the
--      data quality gate runner.
--   2. mart: six conformed dimensions (two of them Slowly Changing
--      Dimension Type 2), one partitioned transaction fact, one
--      unpartitioned accumulating snapshot fact.
--   3. int: the working tables the transform in 03 rebuilds on every run.
--
-- WHAT THIS FILE DOES NOT OWN
--   No source data is read here. 02 is pure structure plus the reference
--   rows that have no source system (the unknown members, the calendar,
--   the stage list, the junk dimension's enumerable combinations).
--   Everything that comes from a feed is loaded by 03.
--
-- RUN ORDER: 01, then 02, then 03, then 04. 01 drops and recreates the
-- schemas, so 02 always builds onto a clean mart.
--
-- THE GRAIN CONTRACT
--   Every table below carries its grain as a real COMMENT ON TABLE, one
--   sentence starting "one row per". A grain that lives only in a README
--   is forgotten within a quarter; a grain that lives in the catalogue is
--   still there when the next engineer runs \d+.
-- =====================================================================

\set ON_ERROR_STOP on
\timing on

-- The reporting timezone must already be pinned by 01, because valid_from
-- in both Type 2 dimensions is derived from a date cast and a date cast is
-- timezone dependent. Asserting it here as well means 02 cannot silently
-- build a dimension against the wrong offset if it is run on its own.
DO $$
BEGIN
    IF current_setting('TimeZone') <> 'Africa/Johannesburg' THEN
        RAISE EXCEPTION 'Reporting timezone is %, expected Africa/Johannesburg. Run 01_staging.sql first.',
            current_setting('TimeZone');
    END IF;
END $$;

-- =====================================================================
-- SECTION 1. util. THE FUNCTIONS THAT MAINTAIN PHYSICAL STRUCTURE
-- =====================================================================

-- ---------------------------------------------------------------------
-- util.fn_ensure_month_partition
--
-- WHY A FUNCTION AND NOT 84 HAND-WRITTEN CREATE TABLE STATEMENTS
--   The load has to be able to create a partition for a month it has
--   never seen, in the middle of the night, without a human editing DDL.
--   Writing the partitions out by hand also means the retention job and
--   the load job disagree about what a partition is called, which is how
--   a fact silently lands in the DEFAULT partition.
--
-- WHAT IT GUARANTEES
--   Idempotent: creating a partition that already exists is a no-op, so
--   the load can call it for every month it is about to touch without
--   first working out which ones are missing.
--   Every partition it creates immediately gets its own extended
--   statistics object, because statistics on the partitioned PARENT were
--   measured to have no effect on per-partition estimates.
--   Every partition it creates is registered in meta.partition_registry,
--   so a reader can see which months exist without reading pg_inherits.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_ensure_month_partition(
    p_month  date,
    p_run_id bigint DEFAULT NULL
)
RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_start     date := date_trunc('month', p_month)::date;
    v_end       date := (date_trunc('month', p_month) + interval '1 month')::date;
    v_suffix    text := to_char(v_start, 'YYYY_MM');
    v_part      text := 'fct_deal_stage_event_' || v_suffix;
    v_stats     text := 'stx_fct_stage_pair_'   || v_suffix;
    v_created   boolean := false;
BEGIN
    IF to_regclass('mart.' || quote_ident(v_part)) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE mart.%I PARTITION OF mart.fct_deal_stage_event FOR VALUES FROM (%L) TO (%L)',
            v_part, v_start, v_end);
        v_created := true;
    END IF;

    -- Extended statistics are created per partition, never on the parent.
    -- MEASURED on the live fact: parent level statistics were accepted and
    -- populated by ANALYZE yet left the per-partition estimates byte
    -- identical (51, 96 and 244 rows against actuals of 419, 924 and
    -- 1,779). The same statistics object on the CHILD corrected 96 to
    -- exactly 924 against an actual 924. The planner estimates each
    -- partition from that partition's own statistics, so a partition
    -- without its own object silently gets bad row estimates, and a bad
    -- row estimate is what flips a hash join to a nested loop.
    IF NOT EXISTS (SELECT 1 FROM pg_statistic_ext
                   WHERE stxnamespace = 'mart'::regnamespace AND stxname = v_stats) THEN
        EXECUTE format(
            'CREATE STATISTICS mart.%I (ndistinct, dependencies, mcv) ON from_stage_sk, to_stage_sk FROM mart.%I',
            v_stats, v_part);
    END IF;

    INSERT INTO meta.partition_registry
        (partition_name, parent_table, range_start, range_end, is_default, created_by_run_id, stats_object)
    VALUES (v_part, 'mart.fct_deal_stage_event', v_start, v_end, false, p_run_id, 'mart.' || v_stats)
    ON CONFLICT (partition_name) DO UPDATE
        SET stats_object = EXCLUDED.stats_object;

    RETURN v_part;
END $$;
COMMENT ON FUNCTION util.fn_ensure_month_partition(date, bigint) IS
'Creates the monthly partition covering p_month if it is absent, together with its own extended statistics object, and registers it. Idempotent, so the load can call it for every month it is about to touch.';

-- ---------------------------------------------------------------------
-- util.fn_refresh_partition_stats
--
-- WHY IT EXISTS SEPARATELY: fn_ensure_month_partition covers partitions
-- this build created. This covers partitions that already existed before
-- the statistics policy did, plus the DEFAULT partition, which is not
-- created by the month maker but is still queried and still needs
-- correlated estimates. DQ-008 fails the run if any populated partition
-- is missing its object, and this function is the fix for that failure.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_refresh_partition_stats()
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    r       record;
    v_stats text;
    v_made  integer := 0;
BEGIN
    FOR r IN
        SELECT c.relname AS part_name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'mart.fct_deal_stage_event'::regclass
        ORDER BY c.relname
    LOOP
        -- Same naming convention as fn_ensure_month_partition, so the two
        -- functions can never create two objects for one partition.
        v_stats := 'stx_fct_stage_pair_' || substr(r.part_name, length('fct_deal_stage_event_') + 1);
        -- Partitions made by the month maker already carry the object it
        -- names; only fill the gaps, and never create a duplicate.
        IF NOT EXISTS (
            SELECT 1 FROM pg_statistic_ext e
            WHERE e.stxrelid = ('mart.' || quote_ident(r.part_name))::regclass
        ) THEN
            EXECUTE format(
                'CREATE STATISTICS mart.%I (ndistinct, dependencies, mcv) ON from_stage_sk, to_stage_sk FROM mart.%I',
                v_stats, r.part_name);
            v_made := v_made + 1;
        END IF;
    END LOOP;
    RETURN v_made;
END $$;
COMMENT ON FUNCTION util.fn_refresh_partition_stats() IS
'Creates the (from_stage_sk, to_stage_sk) extended statistics object on every partition of the event fact that does not already have one, including the DEFAULT partition. Returns the number created.';

-- ---------------------------------------------------------------------
-- util.fn_run_dq_gate
--
-- WHY THE RULES ARE DATA AND THE RUNNER IS CODE
--   dq.check_definition holds each rule's SQL as text, so adding a rule
--   is an INSERT and the rule set is queryable. That only works if
--   exactly one piece of code executes them, records the observation and
--   applies the verdict. This is that code.
--
-- THE GATE CONTRACT
--   Every enabled rule for the layer is executed and its observation is
--   written to dq.check_result whether it passed or failed, because the
--   value of a quality framework is the trend line, not the assertion.
--   Only after all of them have been recorded does a failed rule of
--   severity 'error' raise. That ordering matters: a gate that aborted on
--   the first failure would hide the other eleven observations from the
--   engineer who has to debug the run.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_run_dq_gate(p_layer text, p_run_id bigint)
RETURNS TABLE (check_code text, severity text, observed numeric, is_pass boolean)
LANGUAGE plpgsql AS $$
DECLARE
    r          record;
    v_obs      numeric;
    v_t0       timestamptz;
    v_pass     boolean;
    v_failed   text[] := ARRAY[]::text[];
BEGIN
    FOR r IN
        SELECT d.check_code, d.severity, d.layer, d.sql_expression,
               d.threshold_min, d.threshold_max
        FROM dq.check_definition d
        WHERE d.layer = p_layer AND d.is_enabled
        ORDER BY d.check_code
    LOOP
        v_t0 := clock_timestamp();
        EXECUTE r.sql_expression INTO v_obs;
        v_pass := v_obs IS NOT NULL
              AND (r.threshold_min IS NULL OR v_obs >= r.threshold_min)
              AND (r.threshold_max IS NULL OR v_obs <= r.threshold_max);

        INSERT INTO dq.check_result
            (run_id, check_code, layer, severity, observed_value,
             threshold_min, threshold_max, is_pass, duration_ms)
        VALUES (p_run_id, r.check_code, r.layer, r.severity, v_obs,
                r.threshold_min, r.threshold_max, v_pass,
                EXTRACT(EPOCH FROM (clock_timestamp() - v_t0)) * 1000);

        IF NOT v_pass AND r.severity = 'error' THEN
            v_failed := v_failed || (r.check_code || ' (' || COALESCE(v_obs::text, 'NULL') || ')');
        END IF;

        RETURN QUERY SELECT r.check_code, r.severity, v_obs, v_pass;
    END LOOP;

    IF cardinality(v_failed) > 0 THEN
        RAISE EXCEPTION 'DQ gate % FAILED on severity error rule(s): %',
            p_layer, array_to_string(v_failed, ', ');
    END IF;
END $$;
COMMENT ON FUNCTION util.fn_run_dq_gate(text, bigint) IS
'Executes every enabled rule for one layer, records all observations in dq.check_result, then raises if any severity error rule failed. Recording before raising is deliberate: an engineer debugging a failed run needs all twelve observations, not just the first failure.';

-- =====================================================================
-- SECTION 2. THE SMALL CONFORMED DIMENSIONS
-- =====================================================================
-- These four have no source system worth MERGEing from: a calendar, a
-- fixed list of pipeline stages, and the enumerable cross product of
-- three low cardinality flags. They are seeded here with the structure.
-- Only dim_agency is genuinely fed from the source, and it is loaded in
-- 03 where the rest of the feed-driven work lives.
--
-- SURROGATE KEY POLICY
--   Every dimension needs an unknown member so an unresolved fact lands
--   on "Unknown" instead of vanishing, and an unknown member needs an
--   explicit negative key. GENERATED ALWAYS AS IDENTITY rejects that
--   outright: "cannot insert a non-DEFAULT value into column ... Column
--   is an identity column defined as GENERATED ALWAYS". So the big
--   dimensions use GENERATED BY DEFAULT AS IDENTITY, and these tiny ones
--   use plain integer keys with literal values, which are easier to read
--   in a plan and stable across a rebuild.

-- ---------------------------------------------------------------------
-- dim_stage. 11 rows and every one of them earns its place.
-- stage_sk 0 exists so the from_stage of a deal's first event is a real
-- dimension member rather than a NULL, which keeps the fact's foreign key
-- NOT NULL and keeps funnel queries from having to special case the top
-- of the funnel.
-- ---------------------------------------------------------------------
CREATE TABLE mart.dim_stage (
    stage_sk             integer PRIMARY KEY,
    stage_code           text    UNIQUE,
    stage_name           text    NOT NULL UNIQUE,
    stage_order          integer NOT NULL,
    stage_group          text    NOT NULL,
    is_terminal          boolean NOT NULL,
    is_won               boolean NOT NULL,
    is_lost              boolean NOT NULL,
    is_stalled           boolean NOT NULL,
    target_days_in_stage integer,
    -- A stage is open, or it is exactly one kind of closed. The same invariant
    -- is enforced on fct_deal_pipeline, and enforcing it HERE as well is what
    -- lets the fact derive its four state flags straight from this table
    -- instead of from a hardcoded list of stage keys.
    CONSTRAINT ck_dim_stage_one_terminal_kind CHECK (
        is_terminal = (is_won OR is_lost OR is_stalled)
        AND (is_won::integer + is_lost::integer + is_stalled::integer) <= 1)
);
COMMENT ON TABLE mart.dim_stage IS
'One row per pipeline stage, plus stage_sk 0 for the from_stage of a first event.';
COMMENT ON COLUMN mart.dim_stage.stage_code IS
'The pipeline system''s own code, which is what the event feed sends. This is the dimension''s NATURAL KEY and the staging load joins on it, so a new source code fails the lookup loudly and lands in quarantine instead of being silently mapped to the wrong stage. NULL on stage_sk 0 because "Not applicable" is a warehouse construct that no source system ever emits.';
COMMENT ON COLUMN mart.dim_stage.stage_order IS
'Sort and comparison order. is_forward_move on the fact is derived from this, so a funnel query never hardcodes a stage name.';
COMMENT ON COLUMN mart.dim_stage.is_stalled IS
'Its own flag rather than a second meaning for is_lost. A stalled deal was never decided and a lost deal was, so a business that wants to chase the stalled ones has to be able to select them, and merging the two would make that impossible.';
COMMENT ON COLUMN mart.dim_stage.target_days_in_stage IS
'Service level target. Lets a dwell report show variance against target without the target being buried in a dashboard filter.';

INSERT INTO mart.dim_stage
    (stage_sk, stage_code, stage_name, stage_order, stage_group,
     is_terminal, is_won, is_lost, is_stalled, target_days_in_stage) VALUES
    ( 0, NULL,            'Not applicable', 0,  'Not applicable', false, false, false, false, NULL),
    ( 1, 'RECEIVED',      'Received',       1,  'Intake',         false, false, false, false, 1),
    ( 2, 'TRIAGED',       'Triaged',        2,  'Intake',         false, false, false, false, 3),
    ( 3, 'QUALIFIED',     'Qualified',      3,  'Assessment',     false, false, false, false, 10),
    ( 4, 'UNDERWRITING',  'Underwriting',   4,  'Assessment',     false, false, false, false, 14),
    ( 5, 'OFFER',         'Offer',          5,  'Execution',      false, false, false, false, 21),
    ( 6, 'DUE_DILIGENCE', 'Due diligence',  6,  'Execution',      false, false, false, false, 30),
    ( 7, 'LEGAL',         'Legal',          7,  'Execution',      false, false, false, false, 30),
    ( 8, 'CLOSED_WON',    'Closed won',     8,  'Terminal',       true,  true,  false, false, NULL),
    ( 9, 'LOST',          'Lost',           9,  'Terminal',       true,  false, true,  false, NULL),
    (10, 'STALLED',       'Stalled',        10, 'Terminal',       true,  false, false, true,  NULL);

-- ---------------------------------------------------------------------
-- dim_deal_source. A JUNK DIMENSION.
--
-- WHY: three low cardinality flags (5 channels, an off market boolean,
-- three quality grades) would otherwise sit as three columns on a
-- 1.15 million row fact. Collapsing them into one integer key costs one
-- 31 row table and takes two columns off every fact row, and because the
-- combinations are enumerable the dimension can be seeded complete rather
-- than discovered, which means the fact's foreign key can never fail on a
-- combination nobody thought of.
-- ---------------------------------------------------------------------
CREATE TABLE mart.dim_deal_source (
    deal_source_sk     integer PRIMARY KEY,
    source_channel     text    NOT NULL,
    is_off_market      boolean NOT NULL,
    submission_quality text    NOT NULL,
    CONSTRAINT uq_dim_deal_source UNIQUE (source_channel, is_off_market, submission_quality)
);
COMMENT ON TABLE mart.dim_deal_source IS
'One row per combination of source channel, off market flag and submission quality. Junk dimension, seeded complete because the cross product is enumerable.';
COMMENT ON COLUMN mart.dim_deal_source.submission_quality IS
'clean, repaired (a value or an identifier had to be corrected on the way in) or suspect (value missing or implausible). Lets any report exclude suspect submissions without re-deriving what suspect means.';

INSERT INTO mart.dim_deal_source (deal_source_sk, source_channel, is_off_market, submission_quality)
VALUES (-1, 'Unknown', false, 'Unknown');

INSERT INTO mart.dim_deal_source (deal_source_sk, source_channel, is_off_market, submission_quality)
SELECT (row_number() OVER (ORDER BY c.ord, o.flg, q.ord))::integer,
       c.channel, o.flg, q.quality
FROM (VALUES ('Email intake', 1), ('Portal', 2), ('Referral', 3),
             ('Direct call', 4), ('Auction notice', 5))     AS c(channel, ord)
CROSS JOIN (VALUES (false), (true))                          AS o(flg)
CROSS JOIN (VALUES ('clean', 1), ('repaired', 2), ('suspect', 3)) AS q(quality, ord);

-- ---------------------------------------------------------------------
-- dim_agency. Type 1 conformed OUTRIGGER off dim_broker.
--
-- WHY A SNOWFLAKE IN AN OTHERWISE FLAT STAR: agency attributes change on
-- their own schedule and are needed for agency level rollups. Holding
-- them on dim_broker instead would mean rewriting every historical broker
-- version whenever one agency is reclassified, which is precisely the
-- rewrite a Type 2 dimension exists to avoid.
--
-- dim_broker still carries a denormalised agency_name alongside agency_sk.
-- That redundancy is deliberate, not an oversight: the name is what
-- reports group by, and having it on the broker row means the common case
-- never touches this table at all. The key is there for the cases that
-- need the agency's own attributes or its parent.
--
-- parent_agency_sk makes the group / regional master / local office
-- structure explicit, which is what a recursive CTE rollup needs. It is
-- assigned once per agency and never rewritten (see 03), so the hierarchy
-- is stable across runs.
-- ---------------------------------------------------------------------
CREATE TABLE mart.dim_agency (
    agency_sk             integer PRIMARY KEY,
    agency_name           text    NOT NULL UNIQUE,
    parent_agency_sk      integer REFERENCES mart.dim_agency (agency_sk),
    agency_tier           text    NOT NULL,
    head_office_province  text    NOT NULL,
    is_national           boolean NOT NULL,
    broker_headcount_band text    NOT NULL,
    first_loaded_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_dim_agency_not_own_parent CHECK (parent_agency_sk IS DISTINCT FROM agency_sk)
);
COMMENT ON TABLE mart.dim_agency IS
'One row per broking agency. Type 1 conformed outrigger off dim_broker, self referencing on parent_agency_sk so an agency rollup is a recursive CTE rather than a hardcoded three level join.';
COMMENT ON COLUMN mart.dim_agency.broker_headcount_band IS
'Banded count of brokers seen under this agency in the directory feed. Banded rather than exact because an exact headcount would change every snapshot and turn a Type 1 attribute into a Type 2 problem.';

INSERT INTO mart.dim_agency
    (agency_sk, agency_name, parent_agency_sk, agency_tier, head_office_province, is_national, broker_headcount_band)
VALUES (-1, 'Unknown agency', NULL, 'Unknown', 'Unknown', false, 'Unknown');

-- ---------------------------------------------------------------------
-- dim_date. 3,287 days, 2019-01-01 through 2027-12-31.
--
-- WHY WIDER THAN THE FACTS: one full year before the first fact so a
-- prior year comparison works for the 2020 cohort instead of returning
-- NULL, and 18 months after the last so future dated milestones and
-- forward partitions always have a key to point at.
--
-- WHY date_sk IS yyyymmdd AND NOT A SEQUENCE: a readable key in a 2am
-- debug session is worth more than the three bytes a smallint would save.
-- 20250120 tells you what it is; 2211 does not.
-- ---------------------------------------------------------------------
CREATE TABLE mart.dim_date (
    date_sk              integer PRIMARY KEY,
    date_actual          date    NOT NULL UNIQUE,
    year_num             integer NOT NULL,
    quarter_num          integer NOT NULL,
    month_num            integer NOT NULL,
    month_name           text    NOT NULL,
    month_start_date     date    NOT NULL,
    month_end_date       date    NOT NULL,
    iso_week             integer NOT NULL,
    iso_year             integer NOT NULL,
    day_of_month         integer NOT NULL,
    day_of_week          integer NOT NULL,
    day_name             text    NOT NULL,
    is_weekend           boolean NOT NULL,
    is_za_public_holiday boolean NOT NULL,
    is_summer_shutdown   boolean NOT NULL,
    fiscal_year          integer NOT NULL,
    fiscal_quarter       integer NOT NULL,
    days_in_month        integer NOT NULL,
    prior_year_date_sk   integer
);
COMMENT ON TABLE mart.dim_date IS
'One row per calendar day, 2019-01-01 through 2027-12-31.';
COMMENT ON COLUMN mart.dim_date.is_summer_shutdown IS
'Mid December to mid January, when the South African property market genuinely stops. Having the flag in the dimension is what lets the December collapse in the facts be explained rather than merely observed.';
COMMENT ON COLUMN mart.dim_date.prior_year_date_sk IS
'Same calendar day one year earlier, clamped to the shorter month so 29 February resolves. Turns year over year into an equality join instead of date arithmetic in every query.';
COMMENT ON COLUMN mart.dim_date.fiscal_year IS
'South African tax year, March to February. Labelled by the calendar year it ends in.';

INSERT INTO mart.dim_date
SELECT (EXTRACT(YEAR FROM d) * 10000 + EXTRACT(MONTH FROM d) * 100 + EXTRACT(DAY FROM d))::integer,
       d::date,
       EXTRACT(YEAR    FROM d)::integer,
       EXTRACT(QUARTER FROM d)::integer,
       EXTRACT(MONTH   FROM d)::integer,
       trim(to_char(d, 'Month')),
       date_trunc('month', d)::date,
       (date_trunc('month', d) + interval '1 month - 1 day')::date,
       EXTRACT(WEEK   FROM d)::integer,
       EXTRACT(ISOYEAR FROM d)::integer,
       EXTRACT(DAY    FROM d)::integer,
       EXTRACT(ISODOW FROM d)::integer,
       trim(to_char(d, 'Day')),
       EXTRACT(ISODOW FROM d) >= 6,
       -- Fixed date South African public holidays only. Easter and the
       -- observed-Monday rollover are out of scope and are stated as an
       -- assumption rather than left for a reader to discover.
       to_char(d, 'MM-DD') IN ('01-01','03-21','04-27','05-01','06-16','08-09','09-24','12-16','12-25','12-26'),
       (EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) >= 15)
    OR (EXTRACT(MONTH FROM d) = 1  AND EXTRACT(DAY FROM d) <= 10),
       CASE WHEN EXTRACT(MONTH FROM d) >= 3 THEN EXTRACT(YEAR FROM d) + 1
            ELSE EXTRACT(YEAR FROM d) END::integer,
       (((EXTRACT(MONTH FROM d)::integer + 9) % 12) / 3) + 1,
       EXTRACT(DAY FROM (date_trunc('month', d) + interval '1 month - 1 day'))::integer,
       CASE WHEN EXTRACT(YEAR FROM d) > 2019
            THEN ((EXTRACT(YEAR FROM d) - 1) * 10000
                + EXTRACT(MONTH FROM d) * 100
                + LEAST(EXTRACT(DAY FROM d),
                        EXTRACT(DAY FROM (date_trunc('month', d - interval '1 year')
                                          + interval '1 month - 1 day'))))::integer
       END
FROM generate_series('2019-01-01'::date, '2027-12-31'::date, interval '1 day') AS g(d);

-- QUERY SERVED: the month spine that every monthly aggregate and the dense
-- broker-month materialized view join to. Filtering a month out of 3,287
-- rows is cheap either way, but this index is what lets the spine be an
-- index only scan instead of a heap scan inside a nested loop.
CREATE INDEX ix_dim_date_month ON mart.dim_date (month_start_date, date_actual);

-- =====================================================================
-- SECTION 3. THE TYPE 2 DIMENSIONS. THE LOAD BEARING PIECES.
-- =====================================================================
-- Type 2 exists because a report about the past must describe the past.
-- If a broker moved from Agency 012 to Agency 041 in March 2023, the deals
-- they closed in 2022 still belong to Agency 012, and a Type 1 dimension
-- would silently move that history. The same argument applies to a
-- property rezoned from Industrial to Mixed use: a 2021 deal must still
-- report as Industrial.
--
-- FOUR PHYSICAL DECISIONS, IN ORDER OF HOW MUCH THEY MATTER
--
-- 1. is_current is GENERATED ALWAYS AS (valid_to = 'infinity') STORED.
--    The most common Type 2 defect in production is is_current drifting
--    out of step with valid_to after a manual backfill. A generated column
--    makes that defect unreachable rather than merely unlikely, and it
--    costs nothing: the value is computed once at write time.
--
-- 2. The EXCLUDE constraint is the LOAD BEARING guarantee. It says two
--    versions of one natural key may never cover the same instant. It also
--    guarantees "exactly one current version per key" for free, because
--    two open versions both end at infinity and therefore always overlap.
--    One constraint, two invariants, enforced by the database rather than
--    by the loader being careful.
--
-- 3. The partial unique index is NOT a second enforcement mechanism. It is
--    the loader's access path: every run looks up WHERE is_current and that
--    lookup must not scan the dimension.
--
-- 4. valid_to is NOT NULL DEFAULT 'infinity', never NULL. A NULL valid_to
--    would force every point in time join to write
--    (valid_to IS NULL OR event_ts < valid_to), and the day somebody
--    forgets the first half of that predicate is the day the current
--    version stops matching.
-- =====================================================================

CREATE TABLE mart.dim_broker (
    broker_sk   bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    broker_nk   text        NOT NULL,
    full_name   text        NOT NULL,
    email       text,
    phone       text,
    agency_sk   integer     NOT NULL REFERENCES mart.dim_agency (agency_sk),
    agency_name text        NOT NULL,
    region      text        NOT NULL,
    broker_tier text        NOT NULL,
    is_active   boolean     NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz NOT NULL DEFAULT 'infinity',
    is_current  boolean     GENERATED ALWAYS AS (valid_to = 'infinity'::timestamptz) STORED,
    row_hash    bytea       NOT NULL,
    first_loaded_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_updated_at  timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_dim_broker_window CHECK (valid_to > valid_from),
    CONSTRAINT ex_dim_broker_no_overlap
        EXCLUDE USING gist (broker_nk WITH =, tstzrange(valid_from, valid_to) WITH &&)
);
COMMENT ON TABLE mart.dim_broker IS
'One row per broker per period during which their tracked attributes were unchanged. Slowly Changing Dimension Type 2.';
COMMENT ON COLUMN mart.dim_broker.row_hash IS
'sha256 over the TRACKED attributes only (agency_name, region, broker_tier, is_active), each COALESCEd to a sentinel first. The sentinel matters: concat_ws SKIPS nulls, so without it the attribute sets (Agency A, NULL, Senior) and (Agency A, Senior, NULL) hash identically and a real change becomes invisible to change detection.';
COMMENT ON COLUMN mart.dim_broker.full_name IS
'TYPE 1, overwritten in place and deliberately NOT part of row_hash. A corrected spelling is not a new version of a person, and hashing it would open a spurious version for every typo fix.';
COMMENT ON COLUMN mart.dim_broker.email IS 'TYPE 1, overwritten in place. Not tracked.';
COMMENT ON COLUMN mart.dim_broker.phone IS 'TYPE 1, overwritten in place. Not tracked.';
COMMENT ON COLUMN mart.dim_broker.agency_name IS
'TRACKED, and deliberately denormalised from dim_agency so the common report never has to join the outrigger.';
COMMENT ON COLUMN mart.dim_broker.is_current IS
'GENERATED from valid_to. Cannot drift out of step with the validity window, which is the single most common Type 2 defect in the wild.';
COMMENT ON COLUMN mart.dim_broker.valid_from IS
'Keys present in the FIRST snapshot are anchored at -infinity because their history predates the feed. Genuine later joiners are anchored at their own first snapshot date because they truly did not exist before it, and any fact dated earlier than that resolves to the unknown member by design.';
COMMENT ON CONSTRAINT ex_dim_broker_no_overlap ON mart.dim_broker IS
'LOAD BEARING. Two versions of one broker may never cover the same instant. Also guarantees exactly one current version, because two open versions both end at infinity and therefore always overlap.';

-- ACCESS PATH, NOT ENFORCEMENT. The EXCLUDE constraint above already makes
-- two current rows impossible. This index exists because the SCD2 loader
-- joins WHERE is_current on every run, and on a dimension that grows one
-- version at a time that lookup must be an index scan. Partial, so it
-- indexes about 800 rows instead of 1,200 and stays in cache.
CREATE UNIQUE INDEX uq_dim_broker_current ON mart.dim_broker (broker_nk) WHERE is_current;

-- QUERY SERVED: point in time surrogate key resolution in 03, which asks
-- "which version of this broker was in force at this instant". Column
-- order is deliberate: broker_nk is the equality predicate so it must lead,
-- then valid_from and valid_to are the range predicates. Reversing the
-- order would make the index useless for the equality lookup.
CREATE INDEX ix_dim_broker_pit ON mart.dim_broker (broker_nk, valid_from, valid_to);

-- QUERY SERVED: agency level rollups that start from the outrigger and
-- come back down to brokers.
CREATE INDEX ix_dim_broker_agency ON mart.dim_broker (agency_sk) WHERE is_current;

CREATE TABLE mart.dim_property (
    property_sk    bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    property_nk    text        NOT NULL,
    property_name  text        NOT NULL,
    sector         text        NOT NULL,
    sub_sector     text        NOT NULL,
    province       text        NOT NULL,
    metro          text        NOT NULL,
    building_grade text        NOT NULL,
    gla_sqm_band   text        NOT NULL,
    valid_from     timestamptz NOT NULL,
    valid_to       timestamptz NOT NULL DEFAULT 'infinity',
    is_current     boolean     GENERATED ALWAYS AS (valid_to = 'infinity'::timestamptz) STORED,
    row_hash       bytea       NOT NULL,
    first_loaded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CONSTRAINT ck_dim_property_window CHECK (valid_to > valid_from),
    CONSTRAINT ex_dim_property_no_overlap
        EXCLUDE USING gist (property_nk WITH =, tstzrange(valid_from, valid_to) WITH &&)
);
COMMENT ON TABLE mart.dim_property IS
'One row per property per period during which its tracked attributes were unchanged. Slowly Changing Dimension Type 2, fed from a change-only delta feed rather than a full snapshot.';
COMMENT ON COLUMN mart.dim_property.sector IS
'TRACKED. A rezoning from Industrial to Mixed use is the business event that makes Type 2 mandatory here: a 2021 deal must still report as Industrial.';
COMMENT ON COLUMN mart.dim_property.property_name IS
'TYPE 1, overwritten in place. Brokers type building names freehand, so the register''s spelling is corrected rather than versioned.';
COMMENT ON COLUMN mart.dim_property.gla_sqm_band IS
'TRACKED, and banded rather than exact. An exact square metre figure is re-measured on every survey, which would open a version for noise.';

CREATE UNIQUE INDEX uq_dim_property_current ON mart.dim_property (property_nk) WHERE is_current;
CREATE INDEX ix_dim_property_pit ON mart.dim_property (property_nk, valid_from, valid_to);

-- QUERY SERVED: sector and province slicing of the current estate, which
-- is the entry point for "stalled pipeline value at risk by sector".
CREATE INDEX ix_dim_property_sector ON mart.dim_property (sector, province) WHERE is_current;

-- ---------------------------------------------------------------------
-- THE UNKNOWN MEMBERS.
--
-- WHY THEY ARE STRUCTURE AND NOT DATA: an unresolved fact must land
-- somewhere. If it lands nowhere, warehouse totals stop reconciling to the
-- raw feed and the discrepancy is invisible, because nothing errored. A
-- fact on "Unknown broker" is a number somebody can chase; a fact that
-- silently failed an inner join is not.
--
-- -infinity to infinity, so the unknown member is valid at every instant
-- and the point in time join can never miss it.
-- ---------------------------------------------------------------------
INSERT INTO mart.dim_broker
    (broker_sk, broker_nk, full_name, email, phone, agency_sk, agency_name,
     region, broker_tier, is_active, valid_from, valid_to, row_hash)
VALUES (-1, 'UNKNOWN', 'Unknown broker', NULL, NULL, -1, 'Unknown agency',
        'Unknown', 'Unknown', false, '-infinity', 'infinity',
        sha256(convert_to('UNKNOWN MEMBER', 'UTF8')));

INSERT INTO mart.dim_property
    (property_sk, property_nk, property_name, sector, sub_sector, province,
     metro, building_grade, gla_sqm_band, valid_from, valid_to, row_hash)
VALUES (-1, 'UNKNOWN', 'Unknown property', 'Unknown', 'Unknown', 'Unknown',
        'Unknown', 'Unknown', 'Unknown', '-infinity', 'infinity',
        sha256(convert_to('UNKNOWN MEMBER', 'UTF8')));

-- =====================================================================
-- SECTION 4. mart.fct_deal_stage_event. THE TRANSACTION FACT.
-- =====================================================================
-- GRAIN: one row per accepted deal stage transition. This is the finest
-- grain in the warehouse and the reason funnel and time in stage analysis
-- are possible at all: a status column has no history, so "what fraction
-- of Qualified deals reach Offer, and how long do they take" cannot be
-- asked of one. One row per transition can answer it.
--
-- MEASURE SEMANTICS ARE DOCUMENTED IN COMMENTS BECAUSE GETTING THEM WRONG
-- IS HOW DASHBOARDS LIE. days_in_prev_stage is fully additive.
-- deal_value_zar is NOT additive across events for one deal: summing it
-- without a stage filter counts the same deal once per stage it passed
-- through.
-- =====================================================================
CREATE TABLE mart.fct_deal_stage_event (
    deal_event_sk      bigint GENERATED BY DEFAULT AS IDENTITY,
    -- Degenerate dimensions: real business keys with no attributes of
    -- their own, so they live on the fact rather than in a one column
    -- dimension table nobody would ever join to for its attributes.
    event_nk           text        NOT NULL,
    deal_nk            text        NOT NULL,
    source_message_nk  text        NOT NULL,
    broker_sk          bigint      NOT NULL REFERENCES mart.dim_broker      (broker_sk),
    property_sk        bigint      NOT NULL REFERENCES mart.dim_property    (property_sk),
    deal_source_sk     integer     NOT NULL REFERENCES mart.dim_deal_source (deal_source_sk),
    from_stage_sk      integer     NOT NULL REFERENCES mart.dim_stage       (stage_sk),
    to_stage_sk        integer     NOT NULL REFERENCES mart.dim_stage       (stage_sk),
    event_ts           timestamptz NOT NULL,
    event_date         date        NOT NULL,
    -- DERIVED, NEVER SUPPLIED, so a loader bug cannot land a fact on the
    -- wrong day of the date dimension. The obvious alternative,
    -- to_char(event_date, 'YYYYMMDD')::integer, is rejected outright with
    -- "generation expression is not immutable" because to_char is only
    -- STABLE. EXTRACT on a date is immutable, so this form is legal.
    event_date_sk      integer GENERATED ALWAYS AS (
                           (EXTRACT(YEAR  FROM event_date) * 10000
                          + EXTRACT(MONTH FROM event_date) * 100
                          + EXTRACT(DAY   FROM event_date))::integer) STORED,
    deal_value_zar     numeric(18,2),
    days_in_prev_stage numeric(8,2),
    is_forward_move    boolean     NOT NULL,
    is_terminal_event  boolean     NOT NULL,
    loaded_by_run_id   bigint      NOT NULL,
    -- The partition key has to be in every unique index on a partitioned
    -- table, so it is in both of them. That is a PostgreSQL requirement,
    -- not a modelling preference: "unique constraint on partitioned table
    -- must include all partitioning columns".
    PRIMARY KEY (deal_event_sk, event_date)
) PARTITION BY RANGE (event_date);

COMMENT ON TABLE mart.fct_deal_stage_event IS
'One row per accepted deal stage transition. Transaction fact, the finest grain and the largest table in the warehouse.';
COMMENT ON COLUMN mart.fct_deal_stage_event.event_nk IS
'The source system''s event id. It is what makes the fact load idempotent: the append is an anti join on this key, so a replay inserts nothing. A fact table with no natural key cannot be re-run safely, and a fact that cannot be re-run safely double counts the first time a load is retried.';
COMMENT ON COLUMN mart.fct_deal_stage_event.days_in_prev_stage IS
'FULLY ADDITIVE across every dimension. Populated for EVERY transition including terminal ones, so average time in stage is a real number for Lost and Stalled rather than NULL, and those two are the most interesting stages in the funnel.';
COMMENT ON COLUMN mart.fct_deal_stage_event.deal_value_zar IS
'NON ADDITIVE across events for one deal. Summing it without a to_stage_sk filter counts the same deal once per stage it passed through. Sum it with a stage filter, or read the value from mart.fct_deal_pipeline where the grain is one row per deal.';
COMMENT ON COLUMN mart.fct_deal_stage_event.is_forward_move IS
'Derived from dim_stage.stage_order, never from a hardcoded stage name list, so adding a stage does not silently reclassify history.';
COMMENT ON COLUMN mart.fct_deal_stage_event.event_date_sk IS
'GENERATED from event_date. A loader cannot supply it, so a loader cannot get it wrong.';

-- ---------------------------------------------------------------------
-- PARTITIONS. 84 monthly, 2020-01 through 2026-12, plus one DEFAULT.
--
-- WHY MONTHLY: the reporting grain is month over month funnel, so a month
-- is the unit analysts actually filter on and pruning therefore hits on
-- almost every real query.
--
-- WHY NOT DAILY: about 2,900 partitions. The planner must consider every
-- partition before it can prune, so planning time rises with partition
-- count. That cost was measured on this data at up to 21 ms when no
-- pruning applies against 0.2 ms when it does, which is why the count is
-- capped in the low hundreds. Quoting the cost and not only the win is
-- what makes this a decision rather than a fashion.
--
-- WHY 2026-12 AND NOT 2027-12: the data ends 2026-07. Twelve months of
-- headroom is a load that cannot fail on a slightly future date; another
-- twelve on top of that would be partitions that exist to reach a round
-- number.
--
-- WHY A DEFAULT PARTITION: an out of range event date lands there instead
-- of aborting the load at 3am, and DQ-012 reports its depth every run. A
-- deliberate landing zone with an alarm on it beats a failed load.
-- ---------------------------------------------------------------------
DO $$
DECLARE
    m date := '2020-01-01';
BEGIN
    WHILE m <= '2026-12-01' LOOP
        PERFORM util.fn_ensure_month_partition(m);
        m := (m + interval '1 month')::date;
    END LOOP;
END $$;

CREATE TABLE mart.fct_deal_stage_event_default
    PARTITION OF mart.fct_deal_stage_event DEFAULT;
COMMENT ON TABLE mart.fct_deal_stage_event_default IS
'Deliberate landing zone for event dates outside the declared range. Monitored by DQ-012 with an expected non-zero band, because a partition that is deliberately fed cannot also be asserted empty.';

INSERT INTO meta.partition_registry
    (partition_name, parent_table, range_start, range_end, is_default, stats_object)
VALUES ('fct_deal_stage_event_default', 'mart.fct_deal_stage_event', '-infinity', NULL, true, NULL)
ON CONFLICT (partition_name) DO NOTHING;

-- ---------------------------------------------------------------------
-- THE DECLARED RANGE, AS A VIEW, BECAUSE THREE CONTROLS NEED IT.
--
-- WHY IT IS DERIVED FROM THE CATALOGUE AND NOT HARDCODED: extending the
-- fact by a year is a DDL change, and every control that reasons about the
-- calendar has to follow it automatically or it becomes a false alarm the
-- day somebody adds 2027.
--
-- WHY ANY CONTROL NEEDS IT AT ALL: rows outside this range live in the
-- DEFAULT partition BY DESIGN, and they carry dates that no calendar
-- dimension will ever hold and dwell times that no plausibility ceiling
-- will ever accept. Asserting a calendar invariant against them would be
-- asserting that a deliberate landing zone must be empty. The out of range
-- rows have their own two controls: DQ-012 bands the depth of the DEFAULT
-- partition on every run, and ASSERT-STG-018 reports each offending event.
-- ---------------------------------------------------------------------
CREATE VIEW mart.v_fact_declared_range AS
SELECT min(to_date(right(c.relname, 7), 'YYYY_MM'))                            AS range_start,
       max(to_date(right(c.relname, 7), 'YYYY_MM') + interval '1 month')::date AS range_end
FROM pg_class c
JOIN pg_inherits i ON i.inhrelid = c.oid
WHERE i.inhparent = 'mart.fct_deal_stage_event'::regclass
  AND c.relname ~ '_[0-9]{4}_[0-9]{2}$';
COMMENT ON VIEW mart.v_fact_declared_range IS
'The half open date range covered by the named monthly partitions of the event fact, read from the catalogue. One definition, shared by every control that must exclude the DEFAULT partition rather than assert it empty.';

-- ---------------------------------------------------------------------
-- INDEXES ON THE EVENT FACT. One per proven access path, nothing
-- speculative, and every one of them is checked by make indexcheck
-- against pg_stat_user_indexes after the query suite runs.
--
-- THERE IS NO BRIN INDEX HERE, AND THAT IS A MEASURED DECISION.
-- A BRIN on event_ts was built on this exact 1.14 million row fact and the
-- planner refused it twice: filtering on event_date gave a Seq Scan
-- because partition pruning had already done the work, and filtering on
-- event_ts gave Parallel Append with a Seq Scan on every partition and no
-- index at all. The arithmetic explains it: the average partition is 171
-- pages and the largest is 326, so at the default 128 pages per range a
-- partition holds one or two BRIN ranges and there is nothing to exclude.
-- With enable_seqscan off the planner chose a btree, not the BRIN. BRIN
-- earns its place on the unpartitioned raw landing tables, where it was
-- measured at 24 kB against 48 MB for the equivalent btree, and that is
-- where 01 puts it.
-- ---------------------------------------------------------------------

-- QUERY SERVED: idempotent re-load. The fact append in 04 is an
-- INSERT ... ON CONFLICT (event_nk, event_date) DO NOTHING, so this index IS
-- the idempotence mechanism rather than a backstop for one: a replay offers
-- the same source event ids and the index refuses them. That makes the
-- guarantee physical instead of procedural, which matters because a future
-- script can forget an anti join and cannot forget a unique index.
-- It includes event_date because PostgreSQL requires the partition key in
-- every unique index on a partitioned table. event_nk leads because it is the
-- equality predicate.
CREATE UNIQUE INDEX uq_fct_event_nk ON mart.fct_deal_stage_event (event_nk, event_date);

-- QUERY SERVED: the broker leaderboard and every broker-month aggregate,
-- which filter a date range and group by broker.
-- WHY THIS COLUMN ORDER: broker_sk is the equality or grouping column and
-- event_date is the range, and a composite btree can only use a range
-- predicate on the column after the last equality. Reversed, the index
-- would be a worse copy of partition pruning.
CREATE INDEX ix_fct_event_broker ON mart.fct_deal_stage_event (broker_sk, event_date);

-- QUERY SERVED: a single deal's timeline, and the LAG and LEAD window that
-- derives dwell. Both read every event for one deal in event order, so an
-- index in exactly that order feeds the window without a sort.
CREATE INDEX ix_fct_event_deal ON mart.fct_deal_stage_event (deal_nk, event_ts);

-- QUERY SERVED: win and loss reporting, which reads only terminal events.
-- WHY PARTIAL: terminal events are about a fifth of the fact, so the
-- partial index is a fifth of the size and every page the planner reads is
-- a page it wanted.
-- WHY INCLUDE AND NOT A FOURTH KEY COLUMN: deal_value_zar is returned, not
-- searched. INCLUDE puts it in the leaf pages for an index only scan
-- without paying to keep it sorted.
CREATE INDEX ix_fct_event_terminal ON mart.fct_deal_stage_event (to_stage_sk, event_date)
    INCLUDE (deal_value_zar) WHERE is_terminal_event;

-- =====================================================================
-- SECTION 5. mart.fct_deal_pipeline. THE ACCUMULATING SNAPSHOT FACT.
-- =====================================================================
-- GRAIN: one row per deduplicated deal, UPDATED IN PLACE as milestones
-- land.
--
-- WHY IT IS NOT PARTITIONED, AND WHY THAT IS A CORRECTNESS FIX RATHER
-- THAN A SIMPLIFICATION.
--   Partitioning it by received_date was tried and rejected. PostgreSQL
--   requires the partition key in every unique index, so PRIMARY KEY
--   (deal_nk) is illegal on a partitioned version of this table and the
--   enforced grain silently becomes (deal_nk, received_date). That was
--   proved on the live database: restating one deal's received_date and
--   re-running the MERGE INSERTED a second row, leaving the same deal
--   alive in two partitions at once. Date restatement is a defect this
--   project deliberately seeds, so that is not a hypothetical.
--   At 249,000 rows and about 26 MB there was never a performance case
--   for partitioning it either. Unpartitioned, PRIMARY KEY (deal_nk)
--   enforces the true grain and a restatement updates in place.
--
-- WHY IT EXISTS AT ALL WHEN THE EVENT FACT HOLDS THE SAME INFORMATION:
--   "average days from receipt to offer by cohort quarter" is one indexed
--   scan here against a window function over 1.15 million rows there.
--   The two facts answer different questions on purpose. This one is
--   CURRENT TRUTH and is overwritten; the event fact is the IMMUTABLE
--   AUDIT TRAIL and is never overwritten.
-- =====================================================================
CREATE TABLE mart.fct_deal_pipeline (
    deal_sk              bigint GENERATED BY DEFAULT AS IDENTITY UNIQUE,
    deal_nk              text        NOT NULL PRIMARY KEY,
    source_message_nk    text        NOT NULL,
    broker_sk            bigint      NOT NULL REFERENCES mart.dim_broker      (broker_sk),
    property_sk          bigint      NOT NULL REFERENCES mart.dim_property    (property_sk),
    deal_source_sk       integer     NOT NULL REFERENCES mart.dim_deal_source (deal_source_sk),
    received_date        date        NOT NULL,
    -- The ten milestone timestamps. First time each stage was reached, so
    -- a reopened deal does not overwrite the original milestone.
    received_ts          timestamptz NOT NULL,
    triaged_ts           timestamptz,
    qualified_ts         timestamptz,
    underwriting_ts      timestamptz,
    offer_ts             timestamptz,
    due_diligence_ts     timestamptz,
    legal_ts             timestamptz,
    closed_won_ts        timestamptz,
    lost_ts              timestamptz,
    stalled_ts           timestamptz,
    -- The lag measures. Precomputed because they are the whole reason an
    -- accumulating snapshot is worth maintaining.
    days_to_qualify      numeric(8,2),
    days_to_offer        numeric(8,2),
    days_to_terminal     numeric(8,2),
    stage_count          integer     NOT NULL,
    reopen_count         integer     NOT NULL,
    current_stage_sk     integer     NOT NULL REFERENCES mart.dim_stage (stage_sk),
    is_open              boolean     NOT NULL,
    is_won               boolean     NOT NULL,
    is_lost              boolean     NOT NULL,
    is_stalled           boolean     NOT NULL,
    deal_value_zar       numeric(18,2),
    first_value_zar      numeric(18,2),
    value_revision_count integer     NOT NULL,
    first_loaded_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    last_updated_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    loaded_by_run_id     bigint      NOT NULL,
    -- A deal is open or it is exactly one kind of closed. Enforcing that
-- here means no report has to defend itself against a deal that is both
    -- won and lost.
    CONSTRAINT ck_pipeline_state CHECK (
        (is_open::integer + is_won::integer + is_lost::integer + is_stalled::integer) = 1)
);
COMMENT ON TABLE mart.fct_deal_pipeline IS
'One row per deduplicated deal, updated in place as milestones land. Accumulating snapshot fact. Current truth, as against the event fact which is the immutable audit trail.';
COMMENT ON COLUMN mart.fct_deal_pipeline.deal_value_zar IS
'ADDITIVE across deals at this grain, because the grain is one row per deal. This is the column to sum for pipeline value; the same column on the event fact is not.';
COMMENT ON COLUMN mart.fct_deal_pipeline.first_value_zar IS
'The value as first submitted. Kept alongside the current value so a restatement is measurable rather than merely logged.';
COMMENT ON COLUMN mart.fct_deal_pipeline.value_revision_count IS
'Incremented whenever a MERGE overwrites deal_value_zar with a different figure. The prior value goes to dq.restatement_log, which is the only record that the number used to be something else.';
COMMENT ON COLUMN mart.fct_deal_pipeline.reopen_count IS
'Number of times the deal moved out of a stage it had already reached. Counting it here is what turns "our pipeline is noisy" into a number.';
COMMENT ON COLUMN mart.fct_deal_pipeline.days_to_terminal IS
'Receipt to first terminal event. NULL while the deal is still open, which is correct and is why is_open exists as its own flag rather than being inferred from a NULL.';

-- QUERY SERVED: cohort analysis, which is always "deals received in
-- period X". This is the single most common filter on this table.
CREATE INDEX ix_fct_pipeline_received ON mart.fct_deal_pipeline (received_date);

-- QUERY SERVED: the open pipeline, which is what a deal desk actually
-- looks at.
-- WHY PARTIAL: open deals are a small minority of a table that accumulates
-- forever, so a partial index tracks the working set and does not grow
-- with history. An equivalent full index was measured at 5,512 kB against
-- 224 kB for the partial version on this data.
CREATE INDEX ix_fct_pipeline_open ON mart.fct_deal_pipeline (current_stage_sk, received_date)
    WHERE is_open;

-- QUERY SERVED: per broker win rate and cycle time, which group by broker
-- and read the terminal flags.
CREATE INDEX ix_fct_pipeline_broker ON mart.fct_deal_pipeline (broker_sk) INCLUDE (is_won, deal_value_zar);

-- =====================================================================
-- SECTION 6. int. THE TRANSFORM'S WORKING TABLES.
-- =====================================================================
-- These are DDL, so they live here with the rest of the structure rather
-- than being conjured by CREATE TABLE AS inside the load. That is not
-- tidiness: a CTAS table has no column comments, no NOT NULL constraints
-- and no stable column types, so a transform bug shows up three layers
-- downstream instead of at the point of insert.
--
-- 03 rebuilds them with TRUNCATE and INSERT on every run. They are
-- deliberately NOT append only: they are derived, they are cheap to
-- rebuild, and a stale row in the conform layer is worse than no row.
-- =====================================================================

CREATE TABLE int.deal_deduped (
    deal_nk            text        NOT NULL PRIMARY KEY,
    source_message_nk  text        NOT NULL,
    deal_fingerprint   text        NOT NULL,
    broker_nk          text        NOT NULL,
    property_nk        text        NOT NULL,
    sector             text        NOT NULL,
    received_ts        timestamptz NOT NULL,
    received_date      date        NOT NULL,
    deal_value_zar     numeric(18,2),
    source_channel     text        NOT NULL,
    is_off_market      boolean     NOT NULL,
    submission_quality text        NOT NULL,
    broker_sk          bigint      NOT NULL,
    property_sk        bigint      NOT NULL,
    deal_source_sk     integer     NOT NULL,
    loaded_by_run_id   bigint      NOT NULL
);
COMMENT ON TABLE int.deal_deduped IS
'One row per surviving distinct deal after content fingerprint deduplication, with its surrogate keys already resolved.';
COMMENT ON COLUMN int.deal_deduped.deal_fingerprint IS
'The BLOCKING KEY for deduplication: conformed broker, conformed property, ISO week of receipt. Deliberately NOT the source message id, because a resubmission arrives with a fresh one. Deliberately NOT including the value either, because a resubmitted value is jittered PROPORTIONALLY and no absolute or relative value bucket can absorb that exactly; the value is evidence in dq.duplicate_submission instead. Measured reasoning in 03_load.sql step 9.';
CREATE INDEX ix_int_deal_deduped_fp ON int.deal_deduped (deal_fingerprint);

CREATE TABLE int.stage_event_sequenced (
    event_nk           text        NOT NULL PRIMARY KEY,
    deal_nk            text        NOT NULL,
    source_message_nk  text        NOT NULL,
    from_stage_sk      integer     NOT NULL,
    to_stage_sk        integer     NOT NULL,
    event_ts           timestamptz NOT NULL,
    event_date         date        NOT NULL,
    event_seq          integer     NOT NULL,
    days_in_prev_stage numeric(8,2),
    is_forward_move    boolean     NOT NULL,
    is_terminal_event  boolean     NOT NULL,
    broker_sk          bigint      NOT NULL,
    property_sk        bigint      NOT NULL,
    deal_source_sk     integer     NOT NULL,
    deal_value_zar     numeric(18,2),
    loaded_by_run_id   bigint      NOT NULL
);
COMMENT ON TABLE int.stage_event_sequenced IS
'One row per accepted stage transition, with from_stage and dwell derived by LAG and with surrogate keys resolved point in time. The last stop before the event fact.';
COMMENT ON COLUMN int.stage_event_sequenced.from_stage_sk IS
'LAG(to_stage_sk) over the deal ordered by event_ts then arrival order, COALESCEd to 0 for a first event. The tiebreaker matters: two events on the same timestamp must sequence the same way on every run or the dwell measures move.';
CREATE INDEX ix_int_ses_deal ON int.stage_event_sequenced (deal_nk, event_seq);

CREATE TABLE int.deal_milestone (
    deal_nk          text        NOT NULL PRIMARY KEY,
    received_ts      timestamptz NOT NULL,
    triaged_ts       timestamptz,
    qualified_ts     timestamptz,
    underwriting_ts  timestamptz,
    offer_ts         timestamptz,
    due_diligence_ts timestamptz,
    legal_ts         timestamptz,
    closed_won_ts    timestamptz,
    lost_ts          timestamptz,
    stalled_ts       timestamptz,
    stage_count      integer     NOT NULL,
    reopen_count     integer     NOT NULL,
    current_stage_sk integer     NOT NULL,
    last_event_ts    timestamptz NOT NULL,
    loaded_by_run_id bigint      NOT NULL
);
COMMENT ON TABLE int.deal_milestone IS
'One row per deal with the event stream pivoted into first-time-reached milestone timestamps. Built with aggregate FILTER in one pass, not with ten correlated subqueries.';

ANALYZE mart.dim_stage;
ANALYZE mart.dim_deal_source;
ANALYZE mart.dim_agency;
ANALYZE mart.dim_date;
ANALYZE mart.dim_broker;
ANALYZE mart.dim_property;

\echo ''
\echo '02_warehouse.sql complete. Structure built:'
SELECT 'dimensions'          AS object_class, count(*) AS n FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'mart' AND c.relkind = 'r' AND c.relname LIKE 'dim_%'
UNION ALL
SELECT 'fact partitions', count(*) FROM pg_inherits WHERE inhparent = 'mart.fct_deal_stage_event'::regclass
UNION ALL
SELECT 'partition stats objects', count(*) FROM pg_statistic_ext WHERE stxnamespace = 'mart'::regnamespace
UNION ALL
SELECT 'int working tables', count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'int' AND c.relkind = 'r'
UNION ALL
SELECT 'dim_date rows', count(*) FROM mart.dim_date
UNION ALL
SELECT 'dim_stage rows', count(*) FROM mart.dim_stage
UNION ALL
SELECT 'dim_deal_source rows', count(*) FROM mart.dim_deal_source
ORDER BY 1;
