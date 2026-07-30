-- =====================================================================================
-- 06_performance.sql
-- DealFlow warehouse: the performance engineering lab.
--
-- WHAT THIS FILE IS
--   The single source of truth for the performance case studies. It builds an isolated
--   fixture at full production volume (1,144,830 stage events, 250,000 deals, 800 brokers,
--   2020-01-01 through 2026-07-31), then defines, for each case study, the BEFORE state,
--   the BEFORE query, the fix, and the AFTER query.
--
--   benchmark.py PARSES the "-- @block" directives below and executes them. The SQL and the
--   harness therefore cannot drift apart: there is one copy of every query and every index,
--   and it is this file. Running this file by hand in psql does the same work as the harness,
--   minus the repeated timing runs.
--
-- HOW TO RUN
--   Whole file, by hand:
--     source .env
--     docker exec -i -e PGPASSWORD="$POSTGRES_PASSWORD" dealflow-db \
--         psql -U dealflow -d dealflow -v ON_ERROR_STOP=1 < sql/06_performance.sql
--   Measured, with medians and captured plans:
--     python3 benchmark.py
--
-- WHY THE LAB LIVES IN ITS OWN SCHEMA (perf) AND NOT IN mart
--   Measuring a BEFORE state means dropping the exact indexes the warehouse depends on, and
--   then measuring again after recreating them. Doing that to a loaded mart would mutate a
--   production object mid-flight, reset the index usage counters that mart's own index
--   justification artifact reads out of pg_stat_user_indexes, and make the benchmark unsafe
--   to re-run while anything else is using the warehouse.
--   perf.fct_deal_stage_event and perf.fct_deal_pipeline therefore mirror the mart
--   definitions (same columns, same partitioning, same volume, same physical ordering) in a
--   schema that exists to be broken and rebuilt. Every query below runs unchanged against
--   mart by swapping the schema name; the numbers in PERFORMANCE.md were taken here.
--
-- MEASUREMENT METHOD, STATED ONCE
--   Headline timings are client observed round trip inside one already open psql session,
--   which means they INCLUDE planning time. That is deliberate: planning time is part of what
--   a user waits for, and case study 1 turns out to be as much a planning win as an execution
--   win. Each query is run once to warm the cache and then at least 5 more times; the reported
--   figure is the MEDIAN of the timed runs, with min and max also reported so a reader can see
--   the spread. EXPLAIN (ANALYZE, BUFFERS) is captured separately, for plan SHAPE and buffer
--   counts only, because its per node instrumentation is not free: on an 85 partition plan it
--   inflated one measured query from 3.9 ms to 36.2 ms, roughly 9x. Shape from EXPLAIN, speed
--   from the clock.
--
-- CONTAINER UNDER TEST
--   PostgreSQL 16.14 on aarch64, 8 vCPU, shared_buffers 128MB, work_mem 4MB,
--   max_parallel_workers_per_gather 2. Small work_mem is left at the container default on
--   purpose: it is what makes the disk spill in case study 3 a real observation rather than
--   a manufactured one.
-- =====================================================================================


-- =====================================================================================
-- SECTION 0. PREFLIGHT
-- =====================================================================================
-- @block preflight 000_guard
-- The lab is derived from the LOADED WAREHOUSE in mart, which is the only source that makes
-- the numbers below mean anything: same volume, same value distribution, same physical order.
-- If mart has not been built, stop rather than silently benchmark a toy.
DO $$
DECLARE
    v_events bigint;
BEGIN
    IF to_regclass('mart.fct_deal_stage_event') IS NULL THEN
        RAISE EXCEPTION 'mart.fct_deal_stage_event is missing. Build the warehouse first: sql/01 through sql/04.';
    END IF;
    SELECT count(*) INTO v_events FROM mart.fct_deal_stage_event;
    IF v_events < 1000000 THEN
        RAISE EXCEPTION 'source fixture holds only % events, expected the full volume (about 1.14 million)', v_events;
    END IF;
    RAISE NOTICE 'preflight OK: source fixture holds % stage events', v_events;
END $$;


-- =====================================================================================
-- SECTION 1. THE FIXTURE
-- Idempotent. Every block is safe to re-run and skips work that is already done, so a
-- benchmark re-run costs seconds rather than rebuilding 1.14 million rows.
-- =====================================================================================

-- @block fixture 010_schema_and_helper
CREATE SCHEMA IF NOT EXISTS perf;

-- Deterministic uniform in [0,1) derived from md5(seed || row key). IMMUTABLE and
-- PARALLEL SAFE, and every draw is a pure function of the row's own key rather than of a
-- global generator, so the fixture is identical no matter how many parallel workers the
-- planner uses. setseed() plus random() does NOT have that property under a parallel plan.
CREATE OR REPLACE FUNCTION perf.fn_u(p_seed text, p_key text)
RETURNS double precision LANGUAGE sql IMMUTABLE PARALLEL SAFE AS $$
    SELECT ('x' || substr(md5(p_seed || p_key), 1, 8))::bit(32)::bigint / 4294967296.0;
$$;

-- @block fixture 020_dim_date
CREATE TABLE IF NOT EXISTS perf.dim_date (
    date_sk            integer PRIMARY KEY,
    date_actual        date    NOT NULL UNIQUE,
    year_num           integer NOT NULL,
    quarter_num        integer NOT NULL,
    month_num          integer NOT NULL,
    month_start_date   date    NOT NULL,
    iso_week           integer NOT NULL,
    is_weekend         boolean NOT NULL,
    prior_year_date_sk integer NOT NULL
);
COMMENT ON TABLE perf.dim_date IS 'One row per calendar day, 2019-01-01 through 2027-12-31.';

-- date_sk is the integer yyyymmdd rather than a meaningless sequence, because a key you can
-- read in a debug session is worth more than the three bytes a smaller key would save.
INSERT INTO perf.dim_date
SELECT (EXTRACT(YEAR FROM d) * 10000 + EXTRACT(MONTH FROM d) * 100 + EXTRACT(DAY FROM d))::integer,
       d::date,
       EXTRACT(YEAR    FROM d)::integer,
       EXTRACT(QUARTER FROM d)::integer,
       EXTRACT(MONTH   FROM d)::integer,
       date_trunc('month', d)::date,
       EXTRACT(ISODOW  FROM d)::integer,
       EXTRACT(ISODOW  FROM d) IN (6, 7),
       (EXTRACT(YEAR  FROM d - INTERVAL '1 year') * 10000
      + EXTRACT(MONTH FROM d - INTERVAL '1 year') * 100
      + EXTRACT(DAY   FROM d - INTERVAL '1 year'))::integer
FROM generate_series(DATE '2019-01-01', DATE '2027-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_sk) DO NOTHING;

-- @block fixture 030_dim_stage
CREATE TABLE IF NOT EXISTS perf.dim_stage (
    stage_sk    integer PRIMARY KEY,
    stage_name  text    NOT NULL UNIQUE,
    stage_order integer NOT NULL,
    is_terminal boolean NOT NULL,
    is_won      boolean NOT NULL,
    is_lost     boolean NOT NULL
);
COMMENT ON TABLE perf.dim_stage IS 'One row per pipeline stage, plus stage_sk 0 for the from_stage of a first event.';

-- stage_sk 0 exists so from_stage_sk can be NOT NULL and every funnel query can join rather
-- than outer join. A plain integer key with explicit literals is used instead of an identity
-- column precisely because these values are referenced by number elsewhere.
-- Copied straight from the warehouse, stage_sk 0 included, rather than a hand written
-- 'Not applicable' row. A lab that retypes the dimension it is meant to mirror is a lab that
-- eventually measures a different dimension.
INSERT INTO perf.dim_stage
SELECT stage_sk, stage_name, stage_order, is_terminal, is_won, is_lost
FROM mart.dim_stage
ON CONFLICT (stage_sk) DO NOTHING;

-- @block fixture 040_dim_agency
CREATE TABLE IF NOT EXISTS perf.dim_agency (
    agency_sk            integer PRIMARY KEY,
    agency_name          text    NOT NULL UNIQUE,
    head_office_province text    NOT NULL,
    is_national          boolean NOT NULL
);
COMMENT ON TABLE perf.dim_agency IS 'One row per broking agency. Type 1 outrigger off dim_broker.';

INSERT INTO perf.dim_agency VALUES (-1, 'Unknown agency', 'Unknown', false)
ON CONFLICT (agency_sk) DO NOTHING;

INSERT INTO perf.dim_agency
SELECT ROW_NUMBER() OVER (ORDER BY agency_name)::integer,
       agency_name,
       (ARRAY['Western Cape', 'Gauteng', 'KwaZulu-Natal', 'Eastern Cape'])
           [1 + floor(perf.fn_u('agencyprov', agency_name) * 4)::integer],
       perf.fn_u('national', agency_name) < 0.35
FROM (SELECT DISTINCT agency_name FROM mart.dim_broker WHERE broker_sk <> -1) a
ON CONFLICT (agency_name) DO NOTHING;

-- @block fixture 050_dim_broker
-- SCD Type 2. Surrogate keys are GENERATED BY DEFAULT rather than GENERATED ALWAYS because
-- the unknown member needs the explicit value -1, and GENERATED ALWAYS rejects that with
-- "cannot insert a non-DEFAULT value into column".
CREATE TABLE IF NOT EXISTS perf.dim_broker (
    broker_sk   bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    broker_nk   text        NOT NULL,
    full_name   text        NOT NULL,
    agency_sk   integer     NOT NULL REFERENCES perf.dim_agency (agency_sk),
    agency_name text        NOT NULL,
    region      text        NOT NULL,
    broker_tier text        NOT NULL,
    is_active   boolean     NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz NOT NULL DEFAULT 'infinity',
    is_current  boolean     GENERATED ALWAYS AS (valid_to = 'infinity'::timestamptz) STORED,
    row_hash    bytea       NOT NULL,
    CONSTRAINT ck_dim_broker_window CHECK (valid_to > valid_from),
    -- LOAD BEARING. Two versions of one broker can never cover the same instant. Two rows
    -- both open at infinity always overlap, so this single constraint also enforces
    -- "exactly one current version per broker".
    CONSTRAINT ex_dim_broker_no_overlap
        EXCLUDE USING gist (broker_nk WITH =, tstzrange(valid_from, valid_to) WITH &&)
);
COMMENT ON TABLE perf.dim_broker IS
    'One row per broker per period during which the tracked attributes were unchanged. SCD Type 2.';

-- ACCESS PATH, NOT ENFORCEMENT. The EXCLUDE constraint above already makes two current rows
-- impossible. This index exists so the loader's WHERE is_current lookup does not scan.
CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_broker_current
    ON perf.dim_broker (broker_nk) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_dim_broker_pit
    ON perf.dim_broker (broker_nk, valid_from, valid_to);

INSERT INTO perf.dim_broker (broker_sk, broker_nk, full_name, agency_sk, agency_name,
                             region, broker_tier, is_active, valid_from, valid_to, row_hash)
SELECT -1, 'UNKNOWN', 'Unknown broker', -1, 'Unknown agency', 'Unknown', 'Unknown',
       false, '-infinity', 'infinity', sha256('unknown'::bytea)
WHERE NOT EXISTS (SELECT 1 FROM perf.dim_broker WHERE broker_sk = -1);

INSERT INTO perf.dim_broker (broker_sk, broker_nk, full_name, agency_sk, agency_name,
                             region, broker_tier, is_active, valid_from, valid_to, row_hash)
SELECT s.broker_sk, s.broker_nk, s.full_name,
       -- LEFT JOIN, never INNER. An inner join here would silently drop any broker whose
       -- agency is missing from dim_agency, which is the exact class of silent loss this
       -- warehouse exists to prevent.
       COALESCE(a.agency_sk, -1),
       s.agency_name, s.region, s.broker_tier, s.is_active,
       -- The first version of every key is anchored at -infinity because its history predates
       -- the feed. Anchoring at the first snapshot timestamp instead makes any fact dated
       -- earlier that same day fall out of the point in time join and vanish, without
       -- changing a single row count.
       CASE WHEN s.valid_from = (SELECT min(valid_from) FROM mart.dim_broker)
            THEN '-infinity'::timestamptz
            ELSE s.valid_from END,
       s.valid_to, s.row_hash
FROM mart.dim_broker s
LEFT JOIN perf.dim_agency a ON a.agency_name = s.agency_name
WHERE NOT EXISTS (SELECT 1 FROM perf.dim_broker t WHERE t.broker_sk = s.broker_sk);

SELECT setval(pg_get_serial_sequence('perf.dim_broker', 'broker_sk'),
              (SELECT max(broker_sk) FROM perf.dim_broker));

-- @block fixture 060_dim_property
CREATE TABLE IF NOT EXISTS perf.dim_property (
    property_sk bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    property_nk text        NOT NULL,
    sector      text        NOT NULL,
    province    text        NOT NULL,
    valid_from  timestamptz NOT NULL,
    valid_to    timestamptz NOT NULL DEFAULT 'infinity',
    is_current  boolean     GENERATED ALWAYS AS (valid_to = 'infinity'::timestamptz) STORED,
    CONSTRAINT ck_dim_property_window CHECK (valid_to > valid_from),
    CONSTRAINT ex_dim_property_no_overlap
        EXCLUDE USING gist (property_nk WITH =, tstzrange(valid_from, valid_to) WITH &&)
);
COMMENT ON TABLE perf.dim_property IS
    'One row per property per period during which its tracked attributes were unchanged. SCD Type 2.';

CREATE UNIQUE INDEX IF NOT EXISTS uq_dim_property_current
    ON perf.dim_property (property_nk) WHERE is_current;

INSERT INTO perf.dim_property (property_sk, property_nk, sector, province, valid_from)
SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown', '-infinity'
WHERE NOT EXISTS (SELECT 1 FROM perf.dim_property WHERE property_sk = -1);

INSERT INTO perf.dim_property (property_sk, property_nk, sector, province, valid_from)
SELECT ROW_NUMBER() OVER (ORDER BY p.property_nk)::bigint,
       p.property_nk, p.sector, p.province, '-infinity'
-- The CURRENT version only. perf.dim_property is deliberately one row per property with an
-- open window: the lab measures access paths, and reproducing the warehouse's 1,443 extra
-- historical versions would change the row count without changing a single plan.
FROM (SELECT property_nk, sector, province FROM mart.dim_property
      WHERE is_current AND property_sk <> -1) p
WHERE NOT EXISTS (SELECT 1 FROM perf.dim_property d WHERE d.property_nk = p.property_nk);

SELECT setval(pg_get_serial_sequence('perf.dim_property', 'property_sk'),
              (SELECT max(property_sk) FROM perf.dim_property));

-- @block fixture 070_dim_deal_source
-- Junk dimension: three low cardinality flags collapse into one integer on a 1.15 million row
-- fact instead of three columns, and the combinations are enumerable so they can be filtered.
CREATE TABLE IF NOT EXISTS perf.dim_deal_source (
    deal_source_sk     integer PRIMARY KEY,
    source_channel     text    NOT NULL,
    is_off_market      boolean NOT NULL,
    submission_quality text    NOT NULL,
    UNIQUE (source_channel, is_off_market, submission_quality)
);
COMMENT ON TABLE perf.dim_deal_source IS
    'One row per observed combination of channel, off market flag and submission quality. Junk dimension.';

INSERT INTO perf.dim_deal_source VALUES (-1, 'Unknown', false, 'Unknown')
ON CONFLICT (deal_source_sk) DO NOTHING;

INSERT INTO perf.dim_deal_source
SELECT ROW_NUMBER() OVER (ORDER BY c.source_channel, o.is_off_market, q.submission_quality)::integer,
       c.source_channel, o.is_off_market, q.submission_quality
FROM (SELECT DISTINCT source_channel FROM mart.dim_deal_source WHERE deal_source_sk > 0) c
CROSS JOIN (SELECT unnest(ARRAY[true, false]) AS is_off_market) o
CROSS JOIN (SELECT unnest(ARRAY['clean', 'repaired', 'suspect']) AS submission_quality) q
ON CONFLICT DO NOTHING;

-- @block fixture 080_fact_ddl
-- Foreign keys are deliberately NOT declared inline. They are added in block 100 after the
-- bulk load, which is where the measured finding about NOT VALID on partitioned tables lives.
CREATE TABLE IF NOT EXISTS perf.fct_deal_stage_event (
    deal_event_sk      bigint GENERATED BY DEFAULT AS IDENTITY,
    deal_nk            text        NOT NULL,
    source_message_nk  text        NOT NULL,
    broker_sk          bigint      NOT NULL,
    property_sk        bigint      NOT NULL,
    deal_source_sk     integer     NOT NULL,
    from_stage_sk      integer     NOT NULL,
    to_stage_sk        integer     NOT NULL,
    event_ts           timestamptz NOT NULL,
    event_date         date        NOT NULL,
    -- Derived by the database, never supplied by the loader, so a loader bug cannot land a
    -- fact on the wrong day of the date dimension. The obvious alternative,
    -- to_char(event_date, 'YYYYMMDD')::integer, is REJECTED with "generation expression is
    -- not immutable" because to_char is only STABLE.
    event_date_sk      integer GENERATED ALWAYS AS (
                           (EXTRACT(YEAR  FROM event_date) * 10000
                          + EXTRACT(MONTH FROM event_date) * 100
                          + EXTRACT(DAY   FROM event_date))::integer) STORED,
    deal_value_zar     numeric(18,2),
    days_in_prev_stage numeric(8,2),
    is_forward_move    boolean     NOT NULL,
    is_terminal_event  boolean     NOT NULL,
    -- The partition key has to be in the primary key: PostgreSQL requires every partitioning
    -- column in any unique index on a partitioned table.
    PRIMARY KEY (deal_event_sk, event_date)
) PARTITION BY RANGE (event_date);

COMMENT ON TABLE perf.fct_deal_stage_event IS
    'One row per accepted deal stage transition. Transaction fact, the finest grain in the warehouse.';
COMMENT ON COLUMN perf.fct_deal_stage_event.days_in_prev_stage IS
    'Fully additive across every dimension. Populated for EVERY transition including terminal ones.';
COMMENT ON COLUMN perf.fct_deal_stage_event.deal_value_zar IS
    'NON additive across events for one deal. Summing it without a to_stage_sk filter double counts the same deal at every stage.';

-- 84 monthly partitions, 2020-01 through 2026-12, plus one DEFAULT. Monthly because the
-- reporting grain is month over month funnel, so pruning hits on almost every real query.
-- The partition count is deliberately capped in the low hundreds: planning time grows with
-- it, and section 7 measures that cost rather than only the saving.
DO $$
DECLARE
    v_month date := DATE '2020-01-01';
    v_made  integer := 0;
BEGIN
    WHILE v_month <= DATE '2026-12-01' LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS perf.fct_deal_stage_event_%s '
            'PARTITION OF perf.fct_deal_stage_event FOR VALUES FROM (%L) TO (%L)',
            to_char(v_month, 'YYYY_MM'), v_month, (v_month + INTERVAL '1 month')::date);
        v_made  := v_made + 1;
        v_month := (v_month + INTERVAL '1 month')::date;
    END LOOP;
    -- A deliberate landing zone for out of range dates, so a bad date surfaces as a data
    -- quality alert instead of failing a load at 3am.
    EXECUTE 'CREATE TABLE IF NOT EXISTS perf.fct_deal_stage_event_default '
            'PARTITION OF perf.fct_deal_stage_event DEFAULT';
    RAISE NOTICE 'monthly partitions ensured: %', v_made;
END $$;

-- @block fixture 090_fact_load
DO $$
DECLARE
    v_have bigint;
BEGIN
    SELECT count(*) INTO v_have FROM perf.fct_deal_stage_event;
    IF v_have > 0 THEN
        RAISE NOTICE 'fact already holds % rows, skipping load', v_have;
        RETURN;
    END IF;

    -- Load time only. Every measured query runs at the container default of 4MB, which is
    -- what makes the disk spill in case study 3 a real observation.
    SET LOCAL work_mem = '256MB';

    WITH sequenced AS (
        -- The natural keys are recovered from mart's dimensions rather than read off the
        -- fact, because a star schema fact holds surrogate keys and nothing else. The lab
        -- then resolves them again through its OWN point in time join below, which is the
        -- work being measured: if it simply copied broker_sk across, case study 2 would be
        -- benchmarking an access path that had already been resolved for it.
        SELECT f.deal_nk, b0.broker_nk, p0.property_nk, f.event_ts, f.event_date,
               COALESCE(f.from_stage_sk, 0) AS from_stage_sk,
               f.to_stage_sk, f.deal_value_zar, f.is_forward_move,
               -- The prototype left days_in_prev_stage NULL on terminal transitions, so
               -- average time in stage came out NULL for Lost and Stalled, the two most
               -- interesting stages in the funnel. LAG populates every transition.
               LAG(f.event_ts) OVER (PARTITION BY f.deal_nk
                                     ORDER BY f.event_ts, f.to_stage_sk) AS prev_event_ts
        FROM mart.fct_deal_stage_event f
        JOIN mart.dim_broker   b0 ON b0.broker_sk   = f.broker_sk
        JOIN mart.dim_property p0 ON p0.property_sk = f.property_sk
    )
    INSERT INTO perf.fct_deal_stage_event
        (deal_nk, source_message_nk, broker_sk, property_sk, deal_source_sk,
         from_stage_sk, to_stage_sk, event_ts, event_date, deal_value_zar,
         days_in_prev_stage, is_forward_move, is_terminal_event)
    SELECT s.deal_nk,
           'MSG-' || substr(md5(s.deal_nk || s.event_ts::text), 1, 12),
           -- Surrogate keys are resolved ONCE, here, with the point in time range join, so
           -- every query time join downstream is a plain equality join. COALESCE to the
           -- unknown member means an unresolved key lands on a row instead of disappearing.
           COALESCE(b.broker_sk, -1),
           COALESCE(p.property_sk, -1),
           COALESCE(ds.deal_source_sk, -1),
           s.from_stage_sk, s.to_stage_sk, s.event_ts, s.event_date,
           s.deal_value_zar::numeric(18,2),
           CASE WHEN s.prev_event_ts IS NULL THEN NULL
                ELSE round((EXTRACT(EPOCH FROM (s.event_ts - s.prev_event_ts)) / 86400.0)::numeric, 2)
           END,
           s.is_forward_move,
           st.is_terminal
    FROM sequenced s
    LEFT JOIN perf.dim_broker b
           ON b.broker_nk = s.broker_nk
          AND s.event_ts >= b.valid_from
          AND s.event_ts <  b.valid_to
    LEFT JOIN perf.dim_property p
           ON p.property_nk = s.property_nk AND p.is_current
    -- The junk dimension attributes come from the warehouse's own snapshot fact. The
    -- prototype invented submission_quality from a hash here, which meant the lab's quality
    -- distribution had nothing to do with the warehouse's and any query filtering on it was
    -- measuring fiction.
    LEFT JOIN mart.fct_deal_pipeline dp ON dp.deal_nk = s.deal_nk
    LEFT JOIN mart.dim_deal_source   dm ON dm.deal_source_sk = dp.deal_source_sk
    LEFT JOIN perf.dim_deal_source ds
           ON ds.source_channel     = dm.source_channel
          AND ds.is_off_market      = dm.is_off_market
          AND ds.submission_quality = dm.submission_quality
    JOIN perf.dim_stage st ON st.stage_sk = s.to_stage_sk
    -- Physical order matches event order, which is what an append only fact really looks
    -- like. Any claim about block range summarisation has to be tested against that reality,
    -- not against a randomly ordered table.
    ORDER BY s.event_ts;

    RAISE NOTICE 'fact loaded';
END $$;

-- @block fixture 100_fact_foreign_keys
-- MEASURED FINDING, and it contradicts the design document this lab was built from.
-- The documented pattern is ADD CONSTRAINT ... NOT VALID followed by VALIDATE CONSTRAINT, to
-- keep the exclusive lock window short. On a PARTITIONED parent PostgreSQL 16 refuses it:
--     ERROR:  cannot add NOT VALID foreign key on partitioned table "fct_deal_stage_event"
--             referencing relation "dim_broker"
--     DETAIL:  This feature is not yet supported on partitioned tables.
-- NOT VALID does work on an individual leaf partition, which is a plain table (measured
-- 17.4 ms to add plus 20.1 ms to validate on one 20,182 row partition). But the plain
-- validating ADD CONSTRAINT over all 85 partitions and 1,165,043 rows was re-measured on this
-- build at 3,293 ms for one key, so the per partition workaround buys a shorter lock on each
-- of 85 tables in exchange for 85 separate DDL statements and no guarantee on the parent.
-- Referential integrity across the whole fact for about three seconds is worth taking.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fct_event_broker') THEN
        ALTER TABLE perf.fct_deal_stage_event
            ADD CONSTRAINT fk_fct_event_broker   FOREIGN KEY (broker_sk)      REFERENCES perf.dim_broker (broker_sk),
            ADD CONSTRAINT fk_fct_event_property FOREIGN KEY (property_sk)    REFERENCES perf.dim_property (property_sk),
            ADD CONSTRAINT fk_fct_event_source   FOREIGN KEY (deal_source_sk) REFERENCES perf.dim_deal_source (deal_source_sk),
            ADD CONSTRAINT fk_fct_event_from     FOREIGN KEY (from_stage_sk)  REFERENCES perf.dim_stage (stage_sk),
            ADD CONSTRAINT fk_fct_event_to       FOREIGN KEY (to_stage_sk)    REFERENCES perf.dim_stage (stage_sk);
        RAISE NOTICE 'foreign keys added and validated';
    ELSE
        RAISE NOTICE 'foreign keys already present';
    END IF;
END $$;

-- @block fixture 110_analyze
-- ANALYZE the partitions, not the parent. MEASURED on this container: ANALYZE on the
-- partitioned parent took 7,485 ms, against 128 ms and 165 ms for two single partitions. The
-- planner estimates each partition from that partition's own statistics anyway, so a parent
-- ANALYZE belongs in a weekly maintenance job and not in the load path.
DO $$
DECLARE r record;
BEGIN
    FOR r IN SELECT c.oid::regclass AS part
             FROM pg_class c
             JOIN pg_inherits i ON i.inhrelid = c.oid
             WHERE i.inhparent = 'perf.fct_deal_stage_event'::regclass
    LOOP
        EXECUTE 'ANALYZE ' || r.part;
    END LOOP;
END $$;
ANALYZE perf.dim_broker;
ANALYZE perf.dim_property;
ANALYZE perf.dim_agency;
ANALYZE perf.dim_stage;
ANALYZE perf.dim_date;
ANALYZE perf.dim_deal_source;

-- @block fixture 120_pipeline
-- Accumulating snapshot. NOT PARTITIONED, and that is a correctness decision rather than a
-- simplification. The grain is one row per deal, but PostgreSQL requires the partition key in
-- every unique index, so partitioning by received_date would make PRIMARY KEY (deal_nk)
-- illegal and the stated grain unenforceable: restating a deal's received_date would insert a
-- second row for the same deal instead of updating it. At 42MB the table never needed it.
CREATE TABLE IF NOT EXISTS perf.fct_deal_pipeline (
    deal_nk          text PRIMARY KEY,
    broker_sk        bigint      NOT NULL,
    property_sk      bigint      NOT NULL,
    deal_source_sk   integer     NOT NULL,
    received_date    date        NOT NULL,
    received_ts      timestamptz NOT NULL,
    qualified_ts     timestamptz,
    offer_ts         timestamptz,
    closed_won_ts    timestamptz,
    lost_ts          timestamptz,
    stalled_ts       timestamptz,
    days_to_qualify  numeric(8,2),
    days_to_offer    numeric(8,2),
    days_to_terminal numeric(8,2),
    stage_count      integer     NOT NULL,
    current_stage_sk integer     NOT NULL,
    is_open          boolean     NOT NULL,
    is_won           boolean     NOT NULL,
    is_lost          boolean     NOT NULL,
    is_stalled       boolean     NOT NULL,
    deal_value_zar   numeric(18,2)
);
COMMENT ON TABLE perf.fct_deal_pipeline IS
    'One row per deal, updated in place as milestones land. Accumulating snapshot fact.';

DO $$
DECLARE
    v_have bigint;
BEGIN
    SELECT count(*) INTO v_have FROM perf.fct_deal_pipeline;
    IF v_have > 0 THEN
        RAISE NOTICE 'pipeline fact already holds % rows, skipping load', v_have;
        RETURN;
    END IF;
    SET LOCAL work_mem = '256MB';

    -- One pass over the event fact with aggregate FILTER, rather than one correlated subquery
    -- per milestone. This is why the accumulating snapshot is cheap to build at all.
    INSERT INTO perf.fct_deal_pipeline
    WITH agg AS (
        SELECT e.deal_nk,
               min(e.event_ts)                                              AS received_ts,
               min(e.event_ts) FILTER (WHERE e.to_stage_sk = 3)             AS qualified_ts,
               min(e.event_ts) FILTER (WHERE e.to_stage_sk = 5)             AS offer_ts,
               min(e.event_ts) FILTER (WHERE e.to_stage_sk = 8)             AS closed_won_ts,
               min(e.event_ts) FILTER (WHERE e.to_stage_sk = 9)             AS lost_ts,
               min(e.event_ts) FILTER (WHERE e.to_stage_sk = 10)            AS stalled_ts,
               min(e.event_ts) FILTER (WHERE e.is_terminal_event)           AS terminal_ts,
               count(*)                                                     AS stage_count,
               max(e.deal_value_zar)                                        AS deal_value_zar,
               (array_agg(e.to_stage_sk    ORDER BY e.event_ts DESC))[1]    AS current_stage_sk,
               (array_agg(e.broker_sk      ORDER BY e.event_ts))[1]         AS broker_sk,
               (array_agg(e.property_sk    ORDER BY e.event_ts))[1]         AS property_sk,
               (array_agg(e.deal_source_sk ORDER BY e.event_ts))[1]         AS deal_source_sk
        FROM perf.fct_deal_stage_event e
        GROUP BY e.deal_nk
    )
    SELECT a.deal_nk, a.broker_sk, a.property_sk, a.deal_source_sk,
           a.received_ts::date, a.received_ts,
           a.qualified_ts, a.offer_ts, a.closed_won_ts, a.lost_ts, a.stalled_ts,
           round((EXTRACT(EPOCH FROM (a.qualified_ts - a.received_ts)) / 86400.0)::numeric, 2),
           round((EXTRACT(EPOCH FROM (a.offer_ts     - a.received_ts)) / 86400.0)::numeric, 2),
           round((EXTRACT(EPOCH FROM (a.terminal_ts  - a.received_ts)) / 86400.0)::numeric, 2),
           a.stage_count, a.current_stage_sk,
           a.terminal_ts   IS NULL,
           a.closed_won_ts IS NOT NULL,
           a.lost_ts       IS NOT NULL,
           a.stalled_ts    IS NOT NULL,
           a.deal_value_zar
    FROM agg a;
    RAISE NOTICE 'pipeline fact loaded';
END $$;

CREATE INDEX IF NOT EXISTS ix_pipeline_received ON perf.fct_deal_pipeline (received_date);
ANALYZE perf.fct_deal_pipeline;

-- @block fixture 130_raw_landing
-- Unpartitioned, append only, every column text. This is the one place in the warehouse where
-- a block range index is the right tool, and case study 5 measures it here against the fact.
CREATE TABLE IF NOT EXISTS perf.raw_stage_event (
    ingest_run_id integer     NOT NULL,
    ingested_at   timestamptz NOT NULL,
    raw_line_no   bigint      NOT NULL,
    deal_nk       text,
    stage_code    text,
    event_ts_txt  text,
    value_txt     text,
    payload_hash  text
);
COMMENT ON TABLE perf.raw_stage_event IS
    'One row per stage event payload as received. Append only landing zone, nothing is ever rejected here.';

DO $$
DECLARE
    v_have bigint;
BEGIN
    SELECT count(*) INTO v_have FROM perf.raw_stage_event;
    IF v_have > 0 THEN
        RAISE NOTICE 'raw landing table already holds % rows, skipping load', v_have;
        RETURN;
    END IF;
    SET LOCAL work_mem = '256MB';
    INSERT INTO perf.raw_stage_event
    SELECT 1, e.event_ts, ROW_NUMBER() OVER (ORDER BY e.event_ts),
           e.deal_nk, s.stage_name,
           to_char(e.event_ts, 'YYYY-MM-DD HH24:MI:SS'),
           e.deal_value_zar::text,
           md5(e.deal_nk || e.event_ts::text)
    FROM perf.fct_deal_stage_event e
    JOIN perf.dim_stage s ON s.stage_sk = e.to_stage_sk
    ORDER BY e.event_ts;
    RAISE NOTICE 'raw landing table loaded';
END $$;
ANALYZE perf.raw_stage_event;

-- @block fixture 140_verify
-- The fixture asserts its own shape. If any of these is wrong every number downstream is
-- measuring something other than what it claims to measure.
DO $$
DECLARE
    v_events    bigint;
    v_deals     bigint;
    v_raw       bigint;
    v_parts     bigint;
    v_unknown   bigint;
    v_default   bigint;
    v_src_events bigint;
BEGIN
    SELECT count(*) INTO v_events  FROM perf.fct_deal_stage_event;
    SELECT count(*) INTO v_deals   FROM perf.fct_deal_pipeline;
    SELECT count(*) INTO v_raw     FROM perf.raw_stage_event;
    SELECT count(*) INTO v_default FROM perf.fct_deal_stage_event_default;
    SELECT count(*) INTO v_unknown FROM perf.fct_deal_stage_event
        WHERE broker_sk = -1 OR property_sk = -1;
    SELECT count(*) INTO v_parts FROM pg_inherits
        WHERE inhparent = 'perf.fct_deal_stage_event'::regclass;

    -- ASSERTED AGAINST THE WAREHOUSE, NOT AGAINST TYPED IN NUMBERS.
    -- The first version of this block hardcoded 1144830 and 250000. Those were correct on the
    -- day they were typed and silently wrong the moment the generator changed, which is the
    -- entire failure mode of a self checking fixture that checks itself against a constant.
    -- Comparing to mart proves the thing that actually matters: this lab is measuring the
    -- warehouse and not something that used to resemble it.
    SELECT count(*) INTO v_src_events FROM mart.fct_deal_stage_event;

    ASSERT v_events = v_src_events,
        format('lab holds %s events against %s in mart, so the lab is not a mirror', v_events, v_src_events);
    ASSERT v_raw = v_events,
        format('landing table holds %s rows against %s facts, they are built from each other', v_raw, v_events);
    ASSERT v_deals BETWEEN 240000 AND 260000,
        format('expected roughly 249000 deals, found %s', v_deals);
    ASSERT v_parts >= 85, format('expected at least 85 partitions, found %s', v_parts);
    -- The DEFAULT partition is EXPECTED to hold the out of range events that mart holds too.
    -- Asserting it empty was the prototype's mistake: it made the lab quietly reject the very
    -- defect the warehouse deliberately seeds.
    ASSERT v_default < 5000, format('DEFAULT partition holds %s rows, far more than mart does', v_default);

    RAISE NOTICE 'fixture verified against mart: % events, % deals, % raw rows, % partitions, % in DEFAULT, % on an unknown member',
                 v_events, v_deals, v_raw, v_parts, v_default, v_unknown;
END $$;


-- =====================================================================================
-- SECTION 2. CASE STUDY 1
-- =====================================================================================
-- @case case1
-- @title Non sargable month filter defeats partition pruning
-- @question Quarterly funnel review. For the first quarter of 2026, how many deals moved
--           between each pair of stages, and how long did they sit in the previous stage?
-- @diagnosis The filter wraps the timestamp in to_char() and compares the result to text.
--           The planner cannot reason about a function result against the partition
--           boundaries, so it prunes nothing and reads all 85 partitions, evaluating to_char
--           on all 1,144,830 rows to keep 49,537. The captured plan shows Parallel Append
--           with a Filter on to_char(event_ts) on every partition and Rows Removed by Filter
--           on each one.
-- @fix Rewrite the predicate as a half open range on the partition key itself. No DDL, no
--      index, no new object: the same question asked in a form the planner can prune with.
-- @block before-query case1
SELECT fs.stage_name AS from_stage,
       ts.stage_name AS to_stage,
       count(*)      AS transitions,
       round(avg(e.days_in_prev_stage), 2) AS avg_days_in_prev_stage
FROM perf.fct_deal_stage_event e
JOIN perf.dim_stage fs ON fs.stage_sk = e.from_stage_sk
JOIN perf.dim_stage ts ON ts.stage_sk = e.to_stage_sk
WHERE to_char(e.event_ts, 'YYYY-MM') IN ('2026-01', '2026-02', '2026-03')
GROUP BY 1, 2
ORDER BY 3 DESC;

-- @block after-query case1
SELECT fs.stage_name AS from_stage,
       ts.stage_name AS to_stage,
       count(*)      AS transitions,
       round(avg(e.days_in_prev_stage), 2) AS avg_days_in_prev_stage
FROM perf.fct_deal_stage_event e
JOIN perf.dim_stage fs ON fs.stage_sk = e.from_stage_sk
JOIN perf.dim_stage ts ON ts.stage_sk = e.to_stage_sk
-- Half open, and on event_date because event_date is the partition key. Closed ranges with
-- BETWEEN on a timestamp silently drop or double count the boundary day.
WHERE e.event_date >= DATE '2026-01-01'
  AND e.event_date <  DATE '2026-04-01'
GROUP BY 1, 2
ORDER BY 3 DESC;


-- =====================================================================================
-- SECTION 3. CASE STUDY 2
-- =====================================================================================
-- @case case2
-- @title A broker performance review has no access path
-- @question Broker review. For one broker, across their whole history, how many stage events
--           per year, how many of those were wins, and what value did they close?
-- @diagnosis The query is highly selective (730 of 1,144,830 rows, 0.064 percent) but there
--           is no index on broker_sk, and the filter is not on the partition key so pruning
--           cannot help either. The only plan available is a parallel sequential scan of
--           every partition: about 18,000 buffers read to return 730 rows.
-- @fix A composite btree on (broker_sk, event_date), created on the partitioned parent so
--      every partition inherits it. broker_sk leads because it is the equality predicate;
--      event_date follows so the same index also serves the common "this broker, this
--      period" variant without a second index.
-- @block before-setup case2
DROP INDEX IF EXISTS perf.ix_fct_event_broker_date;
-- @block before-query case2
SELECT d.year_num,
       count(*)                                        AS events,
       count(*) FILTER (WHERE ts.is_won)               AS won_events,
       sum(e.deal_value_zar) FILTER (WHERE ts.is_won)  AS won_value_zar
FROM perf.fct_deal_stage_event e
JOIN perf.dim_date  d  ON d.date_sk   = e.event_date_sk
JOIN perf.dim_stage ts ON ts.stage_sk = e.to_stage_sk
WHERE e.broker_sk = 407
GROUP BY 1
ORDER BY 1;

-- @block after-setup case2
CREATE INDEX IF NOT EXISTS ix_fct_event_broker_date
    ON perf.fct_deal_stage_event (broker_sk, event_date);
-- @block after-query case2
SELECT d.year_num,
       count(*)                                        AS events,
       count(*) FILTER (WHERE ts.is_won)               AS won_events,
       sum(e.deal_value_zar) FILTER (WHERE ts.is_won)  AS won_value_zar
FROM perf.fct_deal_stage_event e
JOIN perf.dim_date  d  ON d.date_sk   = e.event_date_sk
JOIN perf.dim_stage ts ON ts.stage_sk = e.to_stage_sk
WHERE e.broker_sk = 407
GROUP BY 1
ORDER BY 1;


-- =====================================================================================
-- SECTION 4. CASE STUDY 3
-- =====================================================================================
-- @case case3
-- @title Stage dwell percentiles sort 1.14 million rows with no ordered access path
-- @question Cycle time review. Across all history, what is the median and 90th percentile
--           number of days a deal spends in each stage before it moves?
-- @diagnosis Two separate sorts, both spilling. The LAG window needs its input ordered by
--           (deal_nk, event_ts) and nothing provides that order, so the planner sorts all
--           1,144,830 rows: the captured plan shows Sort Method external merge Disk 38144kB
--           inside the window step. percentile_cont then needs its own sort of the same rows
--           by dwell value: external merge Disk 27048kB.
-- @fix A btree on (deal_nk, event_ts). Each partition gets one, and Merge Append reads the
--      85 partitions in key order, so the window function receives a pre-sorted stream and
--      its 48 MB disk sort disappears entirely: temp blocks written fall from 20,713 to
--      8,706. This is a PARTIAL fix and it is the most honest case study in the file. The
--      percentile_cont sort is inherent to the aggregate and cannot be indexed away, so the
--      28 MB spill remains and the wall clock gain is only about 1.2x, with a run to run
--      spread wide enough that one measurement run on a loaded laptop came out SLOWER. The
--      plan improvement is unambiguous and the clock improvement is marginal, which is
--      exactly the situation where quoting only the plan would be misleading. The real fix
--      for this question is not an index at all: it is to materialise the aggregate, which
--      is why mart.mv_broker_month exists.
-- @block before-setup case3
DROP INDEX IF EXISTS perf.ix_fct_event_deal_ts;
-- @block before-query case3
WITH dwell AS (
    SELECT e.to_stage_sk,
           EXTRACT(EPOCH FROM (e.event_ts - LAG(e.event_ts) OVER (
               PARTITION BY e.deal_nk ORDER BY e.event_ts))) / 86400.0 AS days_in_stage
    FROM perf.fct_deal_stage_event e
)
SELECT s.stage_name,
       count(*) AS observations,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY d.days_in_stage)::numeric, 2) AS p50_days,
       round(percentile_cont(0.9) WITHIN GROUP (ORDER BY d.days_in_stage)::numeric, 2) AS p90_days
FROM dwell d
JOIN perf.dim_stage s ON s.stage_sk = d.to_stage_sk
WHERE d.days_in_stage IS NOT NULL
GROUP BY 1
ORDER BY 1;

-- @block after-setup case3
CREATE INDEX IF NOT EXISTS ix_fct_event_deal_ts
    ON perf.fct_deal_stage_event (deal_nk, event_ts);
-- @block after-query case3
WITH dwell AS (
    SELECT e.to_stage_sk,
           EXTRACT(EPOCH FROM (e.event_ts - LAG(e.event_ts) OVER (
               PARTITION BY e.deal_nk ORDER BY e.event_ts))) / 86400.0 AS days_in_stage
    FROM perf.fct_deal_stage_event e
)
SELECT s.stage_name,
       count(*) AS observations,
       round(percentile_cont(0.5) WITHIN GROUP (ORDER BY d.days_in_stage)::numeric, 2) AS p50_days,
       round(percentile_cont(0.9) WITHIN GROUP (ORDER BY d.days_in_stage)::numeric, 2) AS p90_days
FROM dwell d
JOIN perf.dim_stage s ON s.stage_sk = d.to_stage_sk
WHERE d.days_in_stage IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- =====================================================================================
-- SECTION 5. CASE STUDY 4
-- =====================================================================================
-- @case case4
-- @title Open pipeline value at risk reads every deal to report on 4 percent of them
-- @question Risk review. For deals still open and sitting somewhere between Qualified and
--           Legal, what value is exposed by property sector and province?
-- @diagnosis Only 9,427 of 250,000 deals are open, about 3.8 percent, but there is no index
--           on the flag so the planner scans the whole 42MB table and throws away 96 percent
--           of what it read. The plan shows Parallel Seq Scan with Rows Removed by Filter
--           81,360 per worker.
-- @fix A PARTIAL covering index: btree on (current_stage_sk) INCLUDE (property_sk,
--      deal_value_zar) WHERE is_open. Partial because indexing the 96 percent of rows that
--      can never match is wasted space and wasted maintenance on every update. INCLUDE so
--      the two columns the aggregate needs come from the index and the heap is not touched
--      at all, which the plan confirms with Heap Fetches 0.
-- @block before-setup case4
DROP INDEX IF EXISTS perf.ix_pipeline_open_stage;
-- @block before-query case4
SELECT p.sector,
       p.province,
       count(*)                AS open_deals,
       sum(f.deal_value_zar)   AS value_at_risk_zar
FROM perf.fct_deal_pipeline f
JOIN perf.dim_property p ON p.property_sk = f.property_sk
WHERE f.is_open
  AND f.current_stage_sk BETWEEN 3 AND 7
GROUP BY 1, 2
ORDER BY 4 DESC NULLS LAST
LIMIT 20;

-- @block after-setup case4
CREATE INDEX IF NOT EXISTS ix_pipeline_open_stage
    ON perf.fct_deal_pipeline (current_stage_sk)
    INCLUDE (property_sk, deal_value_zar)
    WHERE is_open;
-- @block after-query case4
SELECT p.sector,
       p.province,
       count(*)                AS open_deals,
       sum(f.deal_value_zar)   AS value_at_risk_zar
FROM perf.fct_deal_pipeline f
JOIN perf.dim_property p ON p.property_sk = f.property_sk
WHERE f.is_open
  AND f.current_stage_sk BETWEEN 3 AND 7
GROUP BY 1, 2
ORDER BY 4 DESC NULLS LAST
LIMIT 20;


-- =====================================================================================
-- SECTION 6. CASE STUDY 5. THE ONE THAT WAS REJECTED.
-- =====================================================================================
-- @case case5
-- @title BRIN on the partitioned fact, measured and then rejected
-- @question Reconciliation against the source feed, which reports on timestamps rather than
--           on calendar dates: how many events and what value arrived in March 2026?
-- @diagnosis The filter is on event_ts, and the partition key is event_date, so partition
--           pruning cannot help and the fact is scanned end to end. A block range index on
--           event_ts looks like the obvious answer, because the fact is physically ordered by
--           event_ts and BRIN is cheap.
-- @fix ACCEPTED AS AN INDEX, REJECTED AS THE ANSWER. The index does work: execution drops
--      because each partition's summary excludes it outright. But it adds 85 index relations
--      the planner must consider on every query against this fact, and the measured planning
--      time roughly triples, giving back much of the execution saving. Rewriting the filter
--      onto the partition key is faster than the BRIN fix and costs nothing at all. So BRIN
--      is dropped from the fact and kept only on the unpartitioned raw landing table, where
--      section 8 measures it earning its place. See PERFORMANCE.md for the numbers.
-- @block before-setup case5
DROP INDEX IF EXISTS perf.ix_fct_event_ts_brin;
-- @block before-query case5
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event
WHERE event_ts >= TIMESTAMPTZ '2026-03-01'
  AND event_ts <  TIMESTAMPTZ '2026-04-01';

-- @block after-setup case5
-- Default pages_per_range of 128. The rejected design specified 32 without justifying it,
-- and at the default the index is already 24kB on the raw table.
CREATE INDEX IF NOT EXISTS ix_fct_event_ts_brin
    ON perf.fct_deal_stage_event USING brin (event_ts);
-- @block after-query case5
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event
WHERE event_ts >= TIMESTAMPTZ '2026-03-01'
  AND event_ts <  TIMESTAMPTZ '2026-04-01';


-- =====================================================================================
-- SECTION 6b. THE ALTERNATIVE THAT BEAT THE INDEX
-- =====================================================================================
-- @case case5b
-- @title Free rewrite versus the BRIN index, head to head
-- @question The same March 2026 reconciliation as case 5, and the question that decides
--           whether the index ships: is the best the index can do better than asking the
--           question against the partition key with no index at all?
-- @diagnosis Case 5 shows the BRIN is a real improvement over the naive query, which is
--           exactly how an unnecessary index gets adopted. The comparison that matters is not
--           "index versus no index", it is "index versus the best alternative fix", and the
--           alternative here costs nothing and adds no object to maintain.
-- @fix BEFORE is the naive event_ts filter with the BRIN in place, so the index gets its best
--      case. AFTER is the same question filtered on event_date, the partition key, with the
--      BRIN dropped. The winner decides what ships.
-- @block before-setup case5b
CREATE INDEX IF NOT EXISTS ix_fct_event_ts_brin
    ON perf.fct_deal_stage_event USING brin (event_ts);
ANALYZE perf.fct_deal_stage_event_2026_03;
-- @block before-query case5b
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event
WHERE event_ts >= TIMESTAMPTZ '2026-03-01'
  AND event_ts <  TIMESTAMPTZ '2026-04-01';
-- @block after-setup case5b
DROP INDEX IF EXISTS perf.ix_fct_event_ts_brin;
-- @block after-query case5b
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event
WHERE event_date >= DATE '2026-03-01'
  AND event_date <  DATE '2026-04-01';


-- =====================================================================================
-- SECTION 7. PARTITION PRUNING, PROVEN TWO WAYS
-- =====================================================================================
-- @block evidence 710_pruning_plan_time
-- Plan time pruning. The literals are known while the plan is being built, so the planner
-- discards 82 of the 85 partitions before execution starts. Evidence in the plan: the Append
-- node has exactly three children, named _2026_01, _2026_02 and _2026_03.
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event
WHERE event_date >= DATE '2026-01-01'
  AND event_date <  DATE '2026-04-01';

-- @block evidence 720_pruning_run_time
-- Run time pruning. With a parameterised statement and a forced generic plan the boundaries
-- are NOT known at plan time, so all 85 partitions go into the plan and are eliminated during
-- execution instead. Evidence in the plan: the line "Subplans Removed: 82".
-- This also exposes the honest cost of the partition count: the generic plan touches
-- thousands of catalogue buffers while planning, and planning time rises accordingly.
SET plan_cache_mode = force_generic_plan;
PREPARE pruning_probe(date, date) AS
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event
WHERE event_date >= $1 AND event_date < $2;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE pruning_probe(DATE '2026-01-01', DATE '2026-04-01');
DEALLOCATE pruning_probe;
RESET plan_cache_mode;

-- @block evidence 730_no_pruning
-- The control. No date predicate at all, so nothing can be pruned and all 85 partitions are
-- scanned. Quoting this alongside the pruned figure is what turns partitioning from a
-- fashion into a decision.
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) AS events, sum(deal_value_zar) AS total_value_zar
FROM perf.fct_deal_stage_event;


-- =====================================================================================
-- SECTION 8. SUPPORTING MEASUREMENTS
-- =====================================================================================
-- @block evidence 810_brin_on_raw
-- Where BRIN does belong: one unpartitioned 1,144,830 row heap in ingest order. The whole
-- table is a single 19,841 page relation, so a summary of one range per 128 pages genuinely
-- excludes almost all of it.
DROP INDEX IF EXISTS perf.ix_raw_ingested_brin;
DROP INDEX IF EXISTS perf.ix_raw_ingested_btree;
VACUUM (ANALYZE) perf.raw_stage_event;
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM perf.raw_stage_event
WHERE ingested_at >= TIMESTAMPTZ '2026-03-01' AND ingested_at < TIMESTAMPTZ '2026-04-01';

CREATE INDEX ix_raw_ingested_brin  ON perf.raw_stage_event USING brin (ingested_at);
CREATE INDEX ix_raw_ingested_btree ON perf.raw_stage_event (ingested_at);
ANALYZE perf.raw_stage_event;

-- The size comparison that justifies choosing BRIN over btree here.
SELECT c.relname AS index_name,
       pg_size_pretty(pg_relation_size(c.oid)) AS size,
       pg_relation_size(c.oid) AS bytes
FROM pg_class c
WHERE c.relname IN ('ix_raw_ingested_brin', 'ix_raw_ingested_btree')
ORDER BY 3;

-- Drop the btree so the next plan has to use BRIN or nothing, which is the real choice.
DROP INDEX perf.ix_raw_ingested_btree;
EXPLAIN (ANALYZE, BUFFERS)
SELECT count(*) FROM perf.raw_stage_event
WHERE ingested_at >= TIMESTAMPTZ '2026-03-01' AND ingested_at < TIMESTAMPTZ '2026-04-01';

-- @block evidence 820_partial_index_size
-- The size argument for the partial index in case study 4, measured rather than asserted.
CREATE INDEX IF NOT EXISTS ix_pipeline_open_stage
    ON perf.fct_deal_pipeline (current_stage_sk)
    INCLUDE (property_sk, deal_value_zar) WHERE is_open;
CREATE INDEX IF NOT EXISTS ix_pipeline_stage_full
    ON perf.fct_deal_pipeline (current_stage_sk)
    INCLUDE (property_sk, deal_value_zar);
SELECT c.relname AS index_name,
       pg_size_pretty(pg_relation_size(c.oid)) AS size,
       pg_relation_size(c.oid) AS bytes
FROM pg_class c
WHERE c.relname IN ('ix_pipeline_open_stage', 'ix_pipeline_stage_full')
ORDER BY 3;
-- The full index exists only to be measured against the partial one. It is not part of the
-- warehouse, so it does not survive this script.
DROP INDEX perf.ix_pipeline_stage_full;

-- @block evidence 830_brin_size_on_fact
-- What a BRIN on the partitioned fact actually costs: one index relation per partition.
CREATE INDEX IF NOT EXISTS ix_fct_event_ts_brin
    ON perf.fct_deal_stage_event USING brin (event_ts);
SELECT count(*) AS brin_relations,
       pg_size_pretty(sum(pg_relation_size(c.oid))) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_am am       ON am.oid = c.relam
WHERE n.nspname = 'perf'
  AND am.amname = 'brin'
  AND c.relname LIKE 'fct_deal_stage_event%';
DROP INDEX perf.ix_fct_event_ts_brin;

-- @block evidence 840_analyze_cost
-- The measurement behind "ANALYZE the touched partitions, not the parent".
\timing on
ANALYZE perf.fct_deal_stage_event_2025_03;
ANALYZE perf.fct_deal_stage_event_2026_03;
ANALYZE perf.fct_deal_stage_event;
\timing off


-- =====================================================================================
-- SECTION 9. INDEX JUSTIFICATION
-- Every index has to prove it is used. An index with zero scans after the query suite has run
-- is either dead weight to delete or a missing query to write, and either way it is a finding
-- rather than something to leave lying around. This is the check that caught the BRIN.
-- =====================================================================================
-- @block indexcheck 900_final_state
-- Restore the accepted end state: the four indexes that earned their place, and no BRIN on
-- the fact.
CREATE INDEX IF NOT EXISTS ix_fct_event_broker_date
    ON perf.fct_deal_stage_event (broker_sk, event_date);
CREATE INDEX IF NOT EXISTS ix_fct_event_deal_ts
    ON perf.fct_deal_stage_event (deal_nk, event_ts);
CREATE INDEX IF NOT EXISTS ix_pipeline_open_stage
    ON perf.fct_deal_pipeline (current_stage_sk)
    INCLUDE (property_sk, deal_value_zar) WHERE is_open;
CREATE INDEX IF NOT EXISTS ix_raw_ingested_brin
    ON perf.raw_stage_event USING brin (ingested_at);
DROP INDEX IF EXISTS perf.ix_fct_event_ts_brin;
DROP INDEX IF EXISTS perf.ix_pipeline_stage_full;

-- @block indexcheck 910_usage
-- Partition level index usage rolls up to the index the engineer actually created, so a
-- partitioned index is reported once rather than 85 times.
SELECT COALESCE(parent.relname, s.indexrelname) AS index_name,
       count(*)          AS index_relations,
       sum(s.idx_scan)   AS idx_scan,
       sum(s.idx_tup_read)  AS idx_tup_read,
       sum(s.idx_tup_fetch) AS idx_tup_fetch
FROM pg_stat_user_indexes s
LEFT JOIN pg_inherits inh   ON inh.inhrelid = s.indexrelid
LEFT JOIN pg_class parent   ON parent.oid = inh.inhparent
WHERE s.schemaname = 'perf'
GROUP BY 1
ORDER BY 3 DESC, 1;
