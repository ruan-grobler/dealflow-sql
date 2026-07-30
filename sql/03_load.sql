-- =====================================================================
-- 03_load.sql
-- DealFlow warehouse: the load. raw to stg to int, and the Type 2
-- dimensions.
--
-- WHAT THIS FILE OWNS
--   1. Opening a row in meta.load_run, so every row this run writes can be
--      traced back to it.
--   2. raw to stg: type casting with rejects diverted, never dropped.
--   3. The DQ gate on stg. It must pass before anything reads stg.
--   4. mart.dim_agency (Type 1) and the agency hierarchy.
--   5. mart.dim_broker (Type 2, from a FULL monthly snapshot).
--   6. mart.dim_property (Type 2, from a change only DELTA feed).
--   7. int: conform, deduplicate, sequence, pivot to milestones, and
--      resolve every surrogate key POINT IN TIME.
--   8. The DQ gate on int.
--
-- 04 turns int into the two facts. Nothing here writes to mart.fct_*.
--
-- IDEMPOTENCE, AND WHY IT IS NOT OPTIONAL
--   Running this file twice in a row must leave the database in the same
--   state as running it once. That is what makes a failed 3am load safe to
--   retry, and it is achieved four different ways depending on the table:
--     stg           MERGE on the source key.
--     dq rejects    INSERT ... ON CONFLICT DO NOTHING on the raw address.
--     int           TRUNCATE then INSERT. They are derived and cheap.
--     dim_broker    the change detector compares against what is already
--     dim_property  current, so a second run detects no change and writes
--                   no version. This is the only one of the four that
--                   needed proving rather than asserting, and the proof is
--                   DQ-006 plus the EXCLUDE constraint.
--
-- ONE CODE PATH FOR THE FIRST LOAD AND EVERY LATER LOAD
--   There is no separate initial load script. The watermarks in
--   meta.load_watermark start at -infinity, so the first run reads
--   everything through exactly the same statements a nightly delta run
--   uses. A separate bootstrap script is the classic way for a warehouse
--   to develop two behaviours that drift apart, and the fix is to not have
--   one.
-- =====================================================================

\set ON_ERROR_STOP on
\timing on

SET timezone = 'Africa/Johannesburg';

-- =====================================================================
-- STEP 1. OPEN THE RUN
-- =====================================================================
-- The mode is DERIVED from the watermarks rather than passed in as an
-- argument, because an operator who types INITIAL by hand on a warehouse
-- that already holds data is exactly the accident this column exists to
-- record.
SELECT CASE WHEN min(watermark_ts) = '-infinity'::timestamptz
            THEN 'INITIAL' ELSE 'INCREMENTAL' END AS load_mode
FROM meta.load_watermark
WHERE stream_kind = 'INGEST'
\gset

INSERT INTO meta.load_run (load_mode, notes)
VALUES (:'load_mode', 'sql/03_load.sql: staging, dimensions, intermediate')
RETURNING run_id AS run_id
\gset

-- Published as a session GUC as well as a psql variable. The DO blocks below
-- need it, and a psql variable is substituted client side so it is invisible
-- inside a DO body, which reaches the server as one string literal.
SELECT set_config('dealflow.run_id', :'run_id', false) AS run_id_published;

\echo 'load run opened:' :run_id 'mode' :'load_mode'


-- =====================================================================
-- STEP 2. raw.deal_submission TO stg.deal_submission
-- =====================================================================
-- THE SHAPE OF THIS STEP IS THE WHOLE POINT OF A STAGING LAYER.
--   Classify every candidate row once into a temp table, then read that
--   temp table three times: to reject, to stage, and to count. The
--   alternative, one pass that casts and inserts and catches exceptions
--   row by row, is between 10 and 100 times slower on 262,000 rows and
--   cannot report how many rows it rejected without a second query anyway.
--
-- WHY THE CLASSIFIERS ARE REGEX PREDICATES AND NOT TRY_CAST
--   util.fn_try_ts is PARALLEL UNSAFE, because a plpgsql EXCEPTION block
--   opens a subtransaction and a parallel worker cannot. Mentioning it in
--   this query would force the whole 262,000 row scan to run serially.
--   util.fn_is_real_date and util.fn_is_numeric are IMMUTABLE PARALLEL
--   SAFE and answer the same question, so the plan stays parallel.
-- =====================================================================

DROP TABLE IF EXISTS tmp_submission_cast;
CREATE TEMP TABLE tmp_submission_cast AS
WITH candidate AS (
    -- The ONLY predicate on raw, and the reason raw carries a BRIN index on
    -- ingested_at. On the first run the watermark is -infinity so this reads
    -- everything; on a nightly run it reads one batch.
    SELECT r.*
    FROM raw.deal_submission r
    WHERE r.ingested_at > (SELECT watermark_ts FROM meta.load_watermark
                           WHERE stream_nk = 'raw.deal_submission')
), classified AS (
    SELECT c.*,
           util.fn_is_real_date(c.received_ts)                          AS date_ok,
           c.deal_value_zar IS NULL
             OR util.fn_is_numeric(c.deal_value_zar)                    AS value_ok,
           lower(btrim(c.broker_email))                                 AS email_norm,
           util.fn_norm_ws(c.sector_hint)                               AS sector_ws,
           util.fn_norm_name(c.property_name)                           AS name_norm
    FROM candidate c
)
SELECT c.source_message_nk,
       c.deal_nk,
       c.broker_nk,
       c.email_norm,
       c.property_nk,
       c.property_name,
       c.name_norm,
       -- The sector the BROKER claimed. Case and whitespace repaired, then
       -- matched against the canonical list; anything that does not match is
       -- dropped rather than guessed at, because int reads the sector from the
       -- property register, not from this hint.
       s.sector                                                  AS sector_hint,
       c.deal_value_zar                                          AS value_text,
       CASE WHEN c.value_ok THEN c.deal_value_zar::numeric END    AS value_raw,
       CASE WHEN c.date_ok  THEN c.received_ts::timestamptz END   AS received_ts,
       c.source_channel,
       c.is_off_market = 'Y'                                      AS is_off_market,
       c.date_ok,
       c.value_ok,
       -- Kept so the rejection reason can be specific. "This row failed" is
       -- not an actionable message to send back to a supplier.
       c.received_ts                                              AS received_text,
       c.source_file,
       c.raw_line_no,
       c.source_batch_id,
       c.ingested_at,
       c.property_nk IS NULL                                      AS name_only,
       c.email_norm IS DISTINCT FROM c.broker_email               AS email_repaired,
       s.sector IS NOT NULL
         AND s.sector IS DISTINCT FROM c.sector_hint              AS sector_repaired
FROM classified c
LEFT JOIN (SELECT DISTINCT sector FROM stg.property_register) s
       ON lower(s.sector) = lower(c.sector_ws);

-- The property register is loaded further down, so on the very first run the
-- lookup above finds nothing. That is handled rather than ignored: the sector
-- hint is a nice-to-have on the submission and the AUTHORITATIVE sector comes
-- from the register in int, so a NULL hint costs nothing. Stated here because
-- a reader who spots the ordering deserves an answer, not a shrug.

ANALYZE tmp_submission_cast;

-- ---------------------------------------------------------------------
-- 2a. REJECTS. Diverted with a reason code and the original text.
--
-- ON CONFLICT DO NOTHING on the raw address is what makes a retry safe: a
-- run that died after this insert and before the watermark advanced will
-- offer the same rows again, and they must not be logged twice or DQ-001
-- stops reconciling.
-- ---------------------------------------------------------------------
INSERT INTO dq.reject_deal_submission
    (run_id, reason_code, source_file, raw_line_no, source_message_id,
     deal_ref, offending_value, raw_payload)
SELECT :run_id,
       CASE WHEN NOT c.date_ok  THEN 'UNPARSEABLE_RECEIVED_TS'
            WHEN NOT c.value_ok THEN 'UNPARSEABLE_DEAL_VALUE'
       END,
       c.source_file, c.raw_line_no, c.source_message_nk, c.deal_nk,
       CASE WHEN NOT c.date_ok THEN c.received_text ELSE c.value_text END,
       jsonb_build_object('source_message_nk', c.source_message_nk,
                          'deal_nk',           c.deal_nk,
                          'broker_nk',         c.broker_nk,
                          'property_nk',       c.property_nk,
                          'deal_value_zar',    c.value_text,
                          'received_ts',       c.received_text,
                          'source_batch_id',   c.source_batch_id)
FROM tmp_submission_cast c
WHERE NOT c.date_ok OR NOT c.value_ok
ON CONFLICT (source_file, raw_line_no) DO NOTHING;

-- ---------------------------------------------------------------------
-- 2b. THE SURVIVORS. MERGE, because a re-offered row must update, not
-- duplicate.
--
-- THE VALUE REPAIR POLICY, AND WHERE IT STOPS
--   A negative value is a sign typo on a credit note and abs() is a safe
--   repair, so it is repaired and flagged.
--   A value keyed in thousands is NOT repaired. It is indistinguishable
--   from a genuinely small deal, so repairing it would mean inventing a
--   number. It is graded suspect and DQ-011 counts it. Refusing to guess
--   is the whole difference between cleaning data and fabricating it.
-- ---------------------------------------------------------------------
MERGE INTO stg.deal_submission t
USING (
    SELECT c.source_message_nk,
           c.deal_nk,
           c.broker_nk,
           c.email_norm,
           c.property_nk,
           c.property_name,
           COALESCE(c.name_norm, 'unknown')      AS property_name_norm,
           c.sector_hint,
           abs(c.value_raw)                      AS deal_value_zar,
           c.received_ts,
           (c.received_ts AT TIME ZONE 'Africa/Johannesburg')::date AS received_date,
           c.source_channel,
           c.is_off_market,
           CASE
               WHEN c.value_raw IS NULL OR abs(c.value_raw) < 100000 THEN 'suspect'
               WHEN c.value_raw < 0 OR c.name_only
                    OR c.email_repaired OR c.sector_repaired          THEN 'repaired'
               ELSE 'clean'
           END                                   AS submission_quality,
           -- COALESCE, because a NULL value was never repaired and "we do not
           -- know whether we repaired it" is not a state this column has.
           COALESCE(c.value_raw < 0, false)      AS value_was_repaired,
           c.source_file, c.raw_line_no, c.source_batch_id, c.ingested_at
    FROM tmp_submission_cast c
    WHERE c.date_ok AND c.value_ok
) s ON t.source_message_nk = s.source_message_nk
WHEN MATCHED THEN UPDATE SET
    deal_nk = s.deal_nk, broker_nk = s.broker_nk,
    broker_email_norm = s.email_norm, property_nk = s.property_nk,
    property_name = s.property_name, property_name_norm = s.property_name_norm,
    sector_hint = s.sector_hint, deal_value_zar = s.deal_value_zar,
    received_ts = s.received_ts, received_date = s.received_date,
    source_channel = s.source_channel, is_off_market = s.is_off_market,
    submission_quality = s.submission_quality,
    value_was_repaired = s.value_was_repaired,
    source_file = s.source_file, raw_line_no = s.raw_line_no,
    source_batch_id = s.source_batch_id, ingested_at = s.ingested_at,
    loaded_by_run_id = :run_id
WHEN NOT MATCHED THEN INSERT
    (source_message_nk, deal_nk, broker_nk, broker_email_norm, property_nk,
     property_name, property_name_norm, sector_hint, deal_value_zar,
     received_ts, received_date, source_channel, is_off_market,
     submission_quality, value_was_repaired, source_file, raw_line_no,
     source_batch_id, ingested_at, loaded_by_run_id)
VALUES
    (s.source_message_nk, s.deal_nk, s.broker_nk, s.email_norm, s.property_nk,
     s.property_name, s.property_name_norm, s.sector_hint, s.deal_value_zar,
     s.received_ts, s.received_date, s.source_channel, s.is_off_market,
     s.submission_quality, s.value_was_repaired, s.source_file, s.raw_line_no,
     s.source_batch_id, s.ingested_at, :run_id);


-- =====================================================================
-- STEP 3. raw.stage_event TO stg.stage_event
-- =====================================================================
-- TWO DEFECTS ARE RESOLVED HERE AND BOTH ARE LOGGED, NOT DROPPED.
--   UNKNOWN_STAGE_CODE   the feed sends REVIEW, PENDING_INFO, STG-99 or
--                        ON HOLD. The lookup is an inner join on
--                        dim_stage.stage_code, so an unmapped code cannot
--                        silently become stage 1.
--   REDELIVERED_PAYLOAD  the source resends a payload verbatim, so two raw
--                        rows carry one event id. The first by raw address
--                        is staged and the rest are quarantined, which is
--                        what keeps the DQ-001 arithmetic exact.
-- =====================================================================

DROP TABLE IF EXISTS tmp_event_cast;
CREATE TEMP TABLE tmp_event_cast AS
WITH candidate AS (
    SELECT r.*
    FROM raw.stage_event r
    WHERE r.ingested_at > (SELECT watermark_ts FROM meta.load_watermark
                           WHERE stream_nk = 'raw.stage_event')
)
SELECT c.source_event_nk                                    AS event_nk,
       c.deal_nk,
       upper(btrim(c.stage_code))                           AS stage_nk,
       s.stage_sk                                           AS to_stage_sk,
       util.fn_is_real_date(c.event_ts)                     AS date_ok,
       CASE WHEN util.fn_is_real_date(c.event_ts)
            THEN c.event_ts::timestamptz END                AS event_ts,
       c.event_ts                                           AS event_ts_text,
       c.actor_broker_nk,
       CASE WHEN util.fn_is_numeric(c.stage_value_zar)
            THEN c.stage_value_zar::numeric END             AS stage_value_zar,
       c.source_file, c.raw_line_no, c.source_batch_id, c.ingested_at,
       -- The redelivery test. Ordered by the raw address so the SAME row wins
       -- on every run: ordering by ingested_at alone would be a coin toss
       -- between two rows in one file, and a coin toss makes the load
       -- non deterministic.
       row_number() OVER (PARTITION BY c.source_event_nk
                          ORDER BY c.source_file, c.raw_line_no) AS copy_no
FROM candidate c
LEFT JOIN mart.dim_stage s ON s.stage_code = upper(btrim(c.stage_code));

ANALYZE tmp_event_cast;

INSERT INTO dq.quarantine_stage_event
    (run_id, reason_code, event_nk, deal_nk, stage_nk, event_ts_text,
     detected_layer, source_file, raw_line_no, raw_payload)
SELECT :run_id,
       CASE WHEN NOT e.date_ok           THEN 'UNPARSEABLE_EVENT_TS'
            WHEN e.to_stage_sk IS NULL   THEN 'UNKNOWN_STAGE_CODE'
            ELSE 'REDELIVERED_PAYLOAD'
       END,
       e.event_nk, e.deal_nk, e.stage_nk, e.event_ts_text, 'stg',
       e.source_file, e.raw_line_no,
       jsonb_build_object('event_nk',   e.event_nk,
                          'deal_nk',    e.deal_nk,
                          'stage_code', e.stage_nk,
                          'event_ts',   e.event_ts_text,
                          'source_batch_id', e.source_batch_id)
FROM tmp_event_cast e
WHERE NOT e.date_ok OR e.to_stage_sk IS NULL OR e.copy_no > 1
ON CONFLICT (source_file, raw_line_no) DO NOTHING;

MERGE INTO stg.stage_event t
USING (
    SELECT e.event_nk, e.deal_nk, e.stage_nk, e.to_stage_sk, e.event_ts,
           e.actor_broker_nk, e.stage_value_zar,
           e.source_file, e.raw_line_no, e.source_batch_id, e.ingested_at
    FROM tmp_event_cast e
    WHERE e.date_ok AND e.to_stage_sk IS NOT NULL AND e.copy_no = 1
) s ON t.event_nk = s.event_nk
WHEN MATCHED THEN UPDATE SET
    deal_nk = s.deal_nk, stage_nk = s.stage_nk, to_stage_sk = s.to_stage_sk,
    event_ts = s.event_ts, actor_broker_nk = s.actor_broker_nk,
    stage_value_zar = s.stage_value_zar, source_file = s.source_file,
    raw_line_no = s.raw_line_no, source_batch_id = s.source_batch_id,
    ingested_at = s.ingested_at, loaded_by_run_id = :run_id
WHEN NOT MATCHED THEN INSERT
    (event_nk, deal_nk, stage_nk, to_stage_sk, event_ts, actor_broker_nk,
     stage_value_zar, source_file, raw_line_no, source_batch_id, ingested_at,
     loaded_by_run_id)
VALUES
    (s.event_nk, s.deal_nk, s.stage_nk, s.to_stage_sk, s.event_ts,
     s.actor_broker_nk, s.stage_value_zar, s.source_file, s.raw_line_no,
     s.source_batch_id, s.ingested_at, :run_id);


-- =====================================================================
-- STEP 4. THE TWO DIMENSION FEEDS TO stg
-- =====================================================================
-- Neither of these can reject a row. Every column the warehouse needs is
-- either present or has a defined substitute, so there is nothing to
-- divert: a blank sector becomes 'Unknown', which is a KNOWN state and not
-- an absent one. That is why there is no reject table for these feeds and
-- why DQ-001 does not mention them.
-- =====================================================================

MERGE INTO stg.broker_directory t
USING (
    SELECT r.snapshot_date::date                                AS snapshot_date,
           r.broker_nk,
           r.full_name,
           lower(btrim(r.broker_email))                         AS email_norm,
           r.broker_phone                                       AS phone,
           r.agency_code,
           r.agency_name,
           -- Case and whitespace repaired against the canonical list. An
           -- unrepaired "  COASTAL WEST" would hash differently from
           -- "Coastal West" and open a Type 2 version for a keystroke.
           COALESCE(canon_tier.label,   util.fn_norm_ws(r.agency_tier))   AS agency_tier,
           util.fn_norm_ws(r.agency_head_office_province)                 AS agency_head_office_province,
           r.agency_is_national = 'Y'                                     AS agency_is_national,
           util.fn_norm_ws(r.agency_headcount_band)                       AS agency_headcount_band,
           COALESCE(canon_region.label, util.fn_norm_ws(r.region))         AS region,
           COALESCE(canon_btier.label,  util.fn_norm_ws(r.broker_tier))    AS broker_tier,
           r.is_active = 'Y'                                              AS is_active,
           r.source_file, r.raw_line_no, r.source_batch_id, r.ingested_at
    FROM raw.broker_directory r
    -- The canonical lists live in the query rather than in a lookup table
    -- because they are the CONFORM RULE, not reference data: five regions and
    -- four tiers that the warehouse defines and the source does not own.
    LEFT JOIN (VALUES ('Inland North'), ('Inland South'), ('Coastal West'),
                      ('Coastal East'), ('Interior')) AS canon_region(label)
           ON lower(canon_region.label) = lower(util.fn_norm_ws(r.region))
    LEFT JOIN (VALUES ('Principal'), ('Senior'), ('Mid'), ('Junior'))
              AS canon_btier(label)
           ON lower(canon_btier.label) = lower(util.fn_norm_ws(r.broker_tier))
    LEFT JOIN (VALUES ('National'), ('Regional'), ('Boutique'))
              AS canon_tier(label)
           ON lower(canon_tier.label) = lower(util.fn_norm_ws(r.agency_tier))
    WHERE r.ingested_at > (SELECT watermark_ts FROM meta.load_watermark
                           WHERE stream_nk = 'raw.broker_directory')
) s ON t.snapshot_date = s.snapshot_date
   AND t.broker_nk     = s.broker_nk
   AND t.raw_line_no   = s.raw_line_no
WHEN MATCHED THEN UPDATE SET
    full_name = s.full_name, email_norm = s.email_norm, phone = s.phone,
    agency_code = s.agency_code, agency_name = s.agency_name,
    agency_tier = s.agency_tier,
    agency_head_office_province = s.agency_head_office_province,
    agency_is_national = s.agency_is_national,
    agency_headcount_band = s.agency_headcount_band,
    region = s.region, broker_tier = s.broker_tier, is_active = s.is_active,
    source_file = s.source_file, source_batch_id = s.source_batch_id,
    ingested_at = s.ingested_at, loaded_by_run_id = :run_id
WHEN NOT MATCHED THEN INSERT
    (snapshot_date, broker_nk, full_name, email_norm, phone, agency_code,
     agency_name, agency_tier, agency_head_office_province, agency_is_national,
     agency_headcount_band, region, broker_tier, is_active,
     source_file, raw_line_no, source_batch_id, ingested_at, loaded_by_run_id)
VALUES
    (s.snapshot_date, s.broker_nk, s.full_name, s.email_norm, s.phone,
     s.agency_code, s.agency_name, s.agency_tier, s.agency_head_office_province,
     s.agency_is_national, s.agency_headcount_band, s.region, s.broker_tier,
     s.is_active, s.source_file, s.raw_line_no, s.source_batch_id,
     s.ingested_at, :run_id);

MERGE INTO stg.property_register t
USING (
    SELECT r.effective_date::date                        AS effective_date,
           r.property_nk,
           upper(btrim(r.register_event_type))           AS register_event_type,
           r.property_name,
           util.fn_norm_name(r.property_name)            AS property_name_norm,
           util.fn_norm_ws(r.suburb)                     AS suburb,
           util.fn_norm_ws(r.metro)                      AS metro,
           util.fn_norm_ws(r.province)                   AS province,
           -- 'Unknown' rather than NULL. A property with no sector is a real
           -- and interesting state, and NULL would drop it out of every
           -- sector rollup without anyone noticing.
           COALESCE(canon.label, 'Unknown')              AS sector,
           util.fn_norm_ws(r.sub_sector)                 AS sub_sector,
           util.fn_norm_ws(r.building_grade)             AS building_grade,
           CASE WHEN util.fn_is_numeric(r.gla_sqm)
                THEN r.gla_sqm::numeric::integer END     AS gla_sqm,
           util.fn_norm_ws(r.gla_sqm_band)               AS gla_sqm_band,
           r.source_file, r.raw_line_no, r.source_batch_id, r.ingested_at
    FROM raw.property_register r
    LEFT JOIN (VALUES ('Industrial'), ('Retail'), ('Office'), ('Residential'),
                      ('Land'), ('Mixed use'), ('Hospitality'), ('Healthcare'))
              AS canon(label)
           ON lower(canon.label) = lower(util.fn_norm_ws(r.sector))
    WHERE r.ingested_at > (SELECT watermark_ts FROM meta.load_watermark
                           WHERE stream_nk = 'raw.property_register')
) s ON t.effective_date = s.effective_date
   AND t.property_nk    = s.property_nk
   AND t.raw_line_no    = s.raw_line_no
WHEN MATCHED THEN UPDATE SET
    register_event_type = s.register_event_type,
    property_name = s.property_name, property_name_norm = s.property_name_norm,
    suburb = s.suburb, metro = s.metro, province = s.province,
    sector = s.sector, sub_sector = s.sub_sector,
    building_grade = s.building_grade, gla_sqm = s.gla_sqm,
    gla_sqm_band = s.gla_sqm_band, source_file = s.source_file,
    source_batch_id = s.source_batch_id, ingested_at = s.ingested_at,
    loaded_by_run_id = :run_id
WHEN NOT MATCHED THEN INSERT
    (effective_date, property_nk, register_event_type, property_name,
     property_name_norm, suburb, metro, province, sector, sub_sector,
     building_grade, gla_sqm, gla_sqm_band, source_file, raw_line_no,
     source_batch_id, ingested_at, loaded_by_run_id)
VALUES
    (s.effective_date, s.property_nk, s.register_event_type, s.property_name,
     s.property_name_norm, s.suburb, s.metro, s.province, s.sector,
     s.sub_sector, s.building_grade, s.gla_sqm, s.gla_sqm_band, s.source_file,
     s.raw_line_no, s.source_batch_id, s.ingested_at, :run_id);

ANALYZE stg.deal_submission;
ANALYZE stg.stage_event;
ANALYZE stg.broker_directory;
ANALYZE stg.property_register;


-- =====================================================================
-- STEP 5. THE stg DQ GATE
-- =====================================================================
-- DQ-001 lives here and it is the rule that makes error tolerant loading
-- trustworthy: every raw row offered has to be accounted for as staged,
-- rejected or quarantined. Without it, "we divert bad rows" and "we lose
-- rows" look identical from the outside.
-- =====================================================================
SELECT * FROM util.fn_run_dq_gate('stg', :run_id);


-- =====================================================================
-- STEP 6. mart.dim_agency. TYPE 1 CONFORMED OUTRIGGER.
-- =====================================================================
-- Keys are assigned ONCE and never rewritten, which is why this is an
-- INSERT of unseen agencies plus an UPDATE of attributes, rather than a
-- delete and reload. A dimension whose surrogate keys move breaks every
-- fact row that already points at them.
-- =====================================================================
INSERT INTO mart.dim_agency
    (agency_sk, agency_name, agency_tier, head_office_province,
     is_national, broker_headcount_band)
SELECT COALESCE((SELECT max(agency_sk) FROM mart.dim_agency WHERE agency_sk > 0), 0)
           + row_number() OVER (ORDER BY n.agency_name),
       n.agency_name, n.agency_tier, n.head_office_province,
       n.is_national, n.broker_headcount_band
FROM (
    -- DISTINCT ON takes the agency's attributes from its LATEST snapshot,
    -- because Type 1 means "current value wins".
    SELECT DISTINCT ON (d.agency_name)
           d.agency_name, d.agency_tier,
           d.agency_head_office_province AS head_office_province,
           d.agency_is_national          AS is_national,
           d.agency_headcount_band       AS broker_headcount_band
    FROM stg.broker_directory d
    ORDER BY d.agency_name, d.snapshot_date DESC, d.raw_line_no DESC
) n
WHERE NOT EXISTS (SELECT 1 FROM mart.dim_agency a WHERE a.agency_name = n.agency_name);

UPDATE mart.dim_agency a
SET agency_tier           = n.agency_tier,
    head_office_province  = n.head_office_province,
    is_national           = n.is_national,
    broker_headcount_band = n.broker_headcount_band
FROM (
    SELECT DISTINCT ON (d.agency_name)
           d.agency_name, d.agency_tier,
           d.agency_head_office_province AS head_office_province,
           d.agency_is_national          AS is_national,
           d.agency_headcount_band       AS broker_headcount_band
    FROM stg.broker_directory d
    ORDER BY d.agency_name, d.snapshot_date DESC, d.raw_line_no DESC
) n
WHERE a.agency_name = n.agency_name
  -- Only write a row that actually changed. An UPDATE that sets a column to
  -- the value it already holds still writes a new row version, still bloats
  -- the table and still costs a vacuum.
  AND (a.agency_tier, a.head_office_province, a.is_national, a.broker_headcount_band)
   IS DISTINCT FROM
      (n.agency_tier, n.head_office_province, n.is_national, n.broker_headcount_band);

-- ---------------------------------------------------------------------
-- 6a. THE AGENCY HIERARCHY.
--
-- WHY IT IS DERIVED AND NOT SUPPLIED: the source feed has no parent
-- column. The tier does imply one, and the rule below is deterministic, so
-- the hierarchy is stable across runs and a recursive rollup has something
-- real to walk:
--   National  is a root.
--   Regional  reports to the National in its own province, else to the
--             first National alphabetically.
--   Boutique  reports to the Regional in its own province, else to the
--             first Regional alphabetically.
-- Set once, on rows where it is still NULL, so a hierarchy correction made
-- by hand is never silently reverted by the next load.
-- ---------------------------------------------------------------------
UPDATE mart.dim_agency a
SET parent_agency_sk = p.parent_sk
FROM (
    SELECT c.agency_sk,
           COALESCE(same_province.agency_sk, fallback.agency_sk) AS parent_sk
    FROM mart.dim_agency c
    LEFT JOIN LATERAL (
        SELECT p.agency_sk FROM mart.dim_agency p
        WHERE p.agency_sk > 0
          AND p.agency_tier = CASE c.agency_tier WHEN 'Regional' THEN 'National'
                                                 WHEN 'Boutique' THEN 'Regional' END
          AND p.head_office_province = c.head_office_province
          AND p.agency_sk <> c.agency_sk
        ORDER BY p.agency_name LIMIT 1
    ) same_province ON true
    LEFT JOIN LATERAL (
        SELECT p.agency_sk FROM mart.dim_agency p
        WHERE p.agency_sk > 0
          AND p.agency_tier = CASE c.agency_tier WHEN 'Regional' THEN 'National'
                                                 WHEN 'Boutique' THEN 'Regional' END
          AND p.agency_sk <> c.agency_sk
        ORDER BY p.agency_name LIMIT 1
    ) fallback ON true
    WHERE c.agency_sk > 0 AND c.agency_tier IN ('Regional', 'Boutique')
) p
WHERE a.agency_sk = p.agency_sk
  AND a.parent_agency_sk IS NULL
  AND p.parent_sk IS NOT NULL;

ANALYZE mart.dim_agency;


-- =====================================================================
-- STEP 7. mart.dim_broker. TYPE 2 FROM A FULL MONTHLY SNAPSHOT.
-- =====================================================================
-- THE TECHNIQUE: HASH COMPARE, NOT DIFF.
--   A full snapshot tells you the state, never the change. So the loader
--   hashes the TRACKED attributes of every snapshot row and keeps only the
--   snapshots where that hash differs from the broker's previous snapshot.
--   Those are the version boundaries, and the whole history collapses into
--   them in one window pass rather than one pass per snapshot.
--
-- WHY THE HASH USES A SENTINEL FOR NULL, AND THIS IS A REAL BUG THAT WAS
-- CAUGHT: concat_ws SKIPS null arguments, so (Agency A, NULL, 'Senior')
-- and (Agency A, 'Senior', NULL) produce the identical string and a
-- genuine change becomes invisible to change detection. COALESCEing each
-- attribute to a sentinel first makes the positions unambiguous.
--
-- WHY THE FIRST SNAPSHOT ANCHORS AT -infinity
--   A broker present in the FIRST snapshot the warehouse ever received did
--   not start existing on that date; their history simply predates the
--   feed. Anchoring at -infinity is what makes a 2020 fact resolve to a
--   real dimension version. A broker who genuinely joins later anchors at
--   their own first snapshot, and a fact dated before that resolves to the
--   unknown member, which is correct: the warehouse does not know who they
--   were before the feed named them.
-- =====================================================================

DROP TABLE IF EXISTS tmp_broker_boundary;
CREATE TEMP TABLE tmp_broker_boundary AS
WITH snap AS (
    -- DISTINCT ON resolves the seeded defect where one snapshot lists the same
    -- broker twice. Lowest raw_line_no wins, deterministically, because
    -- "whichever the plan happened to read first" is not a rule.
    SELECT DISTINCT ON (d.broker_nk, d.snapshot_date)
           d.broker_nk, d.snapshot_date, d.full_name, d.email_norm, d.phone,
           d.agency_name, d.region, d.broker_tier, d.is_active
    FROM stg.broker_directory d
    WHERE d.snapshot_date > (SELECT watermark_ts FROM meta.load_watermark
                             WHERE stream_nk = 'mart.dim_broker')::date
    ORDER BY d.broker_nk, d.snapshot_date, d.raw_line_no
), hashed AS (
    SELECT s.*,
           sha256(convert_to(concat_ws('|',
               COALESCE(s.agency_name, '~'),
               COALESCE(s.region,      '~'),
               COALESCE(s.broker_tier, '~'),
               COALESCE(s.is_active::text, '~')), 'UTF8')) AS row_hash
    FROM snap s
), compared AS (
    SELECT h.*,
           lag(h.row_hash) OVER (PARTITION BY h.broker_nk ORDER BY h.snapshot_date) AS prev_hash,
           cur.row_hash                                                             AS current_hash,
           cur.valid_from                                                           AS current_valid_from
    FROM hashed h
    LEFT JOIN mart.dim_broker cur
           ON cur.broker_nk = h.broker_nk AND cur.is_current
)
SELECT c.broker_nk, c.snapshot_date, c.full_name, c.email_norm, c.phone,
       c.agency_name, c.region, c.broker_tier, c.is_active, c.row_hash,
       c.current_valid_from,
       CASE WHEN c.prev_hash IS NULL
                 AND c.current_hash IS NULL
                 AND c.snapshot_date = (SELECT min(snapshot_date) FROM stg.broker_directory)
            THEN '-infinity'::timestamptz
            ELSE c.snapshot_date::timestamptz
       END AS valid_from
FROM compared c
WHERE (c.prev_hash IS NOT NULL AND c.prev_hash <> c.row_hash)
   OR (c.prev_hash IS NULL AND (c.current_hash IS NULL OR c.current_hash <> c.row_hash));

ANALYZE tmp_broker_boundary;

-- Close the outgoing version FIRST. The EXCLUDE constraint would reject the
-- insert otherwise, and that is the constraint doing its job: two versions of
-- one broker may never cover the same instant.
UPDATE mart.dim_broker d
SET valid_to = b.first_new, last_updated_at = clock_timestamp()
FROM (SELECT broker_nk, min(valid_from) AS first_new
      FROM tmp_broker_boundary GROUP BY broker_nk) b
WHERE d.broker_nk = b.broker_nk
  AND d.is_current
  AND d.valid_from < b.first_new;

INSERT INTO mart.dim_broker
    (broker_nk, full_name, email, phone, agency_sk, agency_name, region,
     broker_tier, is_active, valid_from, valid_to, row_hash)
SELECT b.broker_nk, b.full_name, b.email_norm, b.phone,
       COALESCE(a.agency_sk, -1), b.agency_name, b.region, b.broker_tier,
       b.is_active, b.valid_from,
       -- The next boundary for this broker closes this version. No next
       -- boundary means it is the current version, and 'infinity' rather than
       -- NULL is what makes is_current a generated column and every point in
       -- time predicate a single half open range test.
       COALESCE(lead(b.valid_from) OVER (PARTITION BY b.broker_nk ORDER BY b.valid_from),
                'infinity'::timestamptz),
       b.row_hash
FROM tmp_broker_boundary b
LEFT JOIN mart.dim_agency a ON a.agency_name = b.agency_name;

-- ---------------------------------------------------------------------
-- 7a. THE TYPE 1 ATTRIBUTES, OVERWRITTEN ACROSS EVERY VERSION.
--
-- full_name, email and phone are deliberately NOT in row_hash. A corrected
-- spelling is not a new version of a person, and hashing it would open a
-- spurious version for every typo fix. Type 1 means the current value
-- appears on every historical row, so this UPDATE is the other half of
-- that decision and not an afterthought.
-- ---------------------------------------------------------------------
UPDATE mart.dim_broker d
SET full_name = l.full_name, email = l.email_norm, phone = l.phone,
    last_updated_at = clock_timestamp()
FROM (
    SELECT DISTINCT ON (s.broker_nk) s.broker_nk, s.full_name, s.email_norm, s.phone
    FROM stg.broker_directory s
    ORDER BY s.broker_nk, s.snapshot_date DESC, s.raw_line_no
) l
WHERE d.broker_nk = l.broker_nk
  AND d.broker_sk <> -1
  AND (d.full_name, d.email, d.phone) IS DISTINCT FROM (l.full_name, l.email_norm, l.phone);

ANALYZE mart.dim_broker;


-- =====================================================================
-- STEP 8. mart.dim_property. TYPE 2 FROM A CHANGE ONLY DELTA FEED.
-- =====================================================================
-- SAME OUTPUT, DIFFERENT SOURCE SHAPE, AND THAT IS WHY BOTH EXIST.
--   The directory sends every broker every month. The register sends one
--   row only when something changed. The hash compare is IDENTICAL code,
--   and it has to be, because a delta feed cannot be trusted to only
--   contain real changes: a RECLASSIFY row that changed nothing tracked
--   must not open a version, and the hash is what proves it did not.
--
--   valid_from is the register's own effective_date, not the ingest date.
--   The register knows when a rezoning took effect; the warehouse only
--   knows when it heard about it, and a fact dated in between must resolve
--   to the version that was legally in force.
-- =====================================================================

DROP TABLE IF EXISTS tmp_property_boundary;
CREATE TEMP TABLE tmp_property_boundary AS
WITH src AS (
    SELECT DISTINCT ON (p.property_nk, p.effective_date)
           p.property_nk, p.effective_date, p.property_name, p.sector,
           p.sub_sector, p.province, p.metro, p.building_grade, p.gla_sqm_band
    FROM stg.property_register p
    WHERE p.effective_date > (SELECT watermark_ts FROM meta.load_watermark
                              WHERE stream_nk = 'mart.dim_property')::date
    ORDER BY p.property_nk, p.effective_date, p.raw_line_no
), hashed AS (
    SELECT s.*,
           sha256(convert_to(concat_ws('|',
               COALESCE(s.sector,         '~'),
               COALESCE(s.sub_sector,     '~'),
               COALESCE(s.province,       '~'),
               COALESCE(s.metro,          '~'),
               COALESCE(s.building_grade, '~'),
               COALESCE(s.gla_sqm_band,   '~')), 'UTF8')) AS row_hash
    FROM src s
), compared AS (
    SELECT h.*,
           lag(h.row_hash) OVER (PARTITION BY h.property_nk ORDER BY h.effective_date) AS prev_hash,
           cur.row_hash AS current_hash
    FROM hashed h
    LEFT JOIN mart.dim_property cur
           ON cur.property_nk = h.property_nk AND cur.is_current
)
SELECT c.property_nk, c.effective_date, c.property_name, c.sector, c.sub_sector,
       c.province, c.metro, c.building_grade, c.gla_sqm_band, c.row_hash,
       CASE WHEN c.prev_hash IS NULL
                 AND c.current_hash IS NULL
                 AND c.effective_date = (SELECT min(effective_date) FROM stg.property_register)
            THEN '-infinity'::timestamptz
            ELSE c.effective_date::timestamptz
       END AS valid_from
FROM compared c
WHERE (c.prev_hash IS NOT NULL AND c.prev_hash <> c.row_hash)
   OR (c.prev_hash IS NULL AND (c.current_hash IS NULL OR c.current_hash <> c.row_hash));

ANALYZE tmp_property_boundary;

UPDATE mart.dim_property d
SET valid_to = b.first_new, last_updated_at = clock_timestamp()
FROM (SELECT property_nk, min(valid_from) AS first_new
      FROM tmp_property_boundary GROUP BY property_nk) b
WHERE d.property_nk = b.property_nk
  AND d.is_current
  AND d.valid_from < b.first_new;

INSERT INTO mart.dim_property
    (property_nk, property_name, sector, sub_sector, province, metro,
     building_grade, gla_sqm_band, valid_from, valid_to, row_hash)
SELECT b.property_nk, b.property_name, b.sector, b.sub_sector, b.province,
       b.metro, b.building_grade, b.gla_sqm_band, b.valid_from,
       COALESCE(lead(b.valid_from) OVER (PARTITION BY b.property_nk ORDER BY b.valid_from),
                'infinity'::timestamptz),
       b.row_hash
FROM tmp_property_boundary b;

UPDATE mart.dim_property d
SET property_name = l.property_name, last_updated_at = clock_timestamp()
FROM (
    SELECT DISTINCT ON (p.property_nk) p.property_nk, p.property_name
    FROM stg.property_register p
    ORDER BY p.property_nk, p.effective_date DESC, p.raw_line_no
) l
WHERE d.property_nk = l.property_nk
  AND d.property_sk <> -1
  AND d.property_name IS DISTINCT FROM l.property_name;

ANALYZE mart.dim_property;


-- =====================================================================
-- STEP 9. int.deal_deduped. CONFORM, THEN DEDUPLICATE.
-- =====================================================================
-- THE ORDER MATTERS: conform first, dedupe second. The fingerprint is
-- built from the CONFORMED broker and property, so two submissions that
-- named the same building two different ways are recognised as the same
-- deal. Deduping first would leave both alive.
--
-- THE FINGERPRINT IS CONTENT, NOT THE MESSAGE ID.
--   A resubmission arrives with a fresh message id, a fresh deal
--   reference, a later timestamp and a value jittered by up to 2 percent.
--   Every identifier is different, so an identifier based dedupe catches
--   NONE of them. The fingerprint is therefore a BLOCKING KEY over content:
--   the conformed broker, the conformed property, and the ISO week the
--   submission was received in.
--
-- WHY THE VALUE IS NOT IN THE KEY. THIS IS THE OPPOSITE OF THE OBVIOUS
-- DESIGN AND IT WAS CHANGED BECAUSE IT WAS MEASURED.
--   The first version bucketed the value to the nearest R100,000 and put
--   the bucket in the key. It caught 4,588 of about 9,600 resubmissions,
--   because the jitter is PROPORTIONAL and the bucket was ABSOLUTE: 2
--   percent of a R30 million deal is R600,000, which is six buckets wide. A
--   relative bucket does not fix it either, it only moves the problem to the
--   bucket boundaries, which the jitter straddles.
--   So the value comes OUT of the key and becomes EVIDENCE instead.
--   dq.duplicate_submission.value_delta_pct records how far each suppressed
--   row sat from its original, and that is the column a reviewer uses to
--   audit the decision. A key has to be exact. Evidence does not.
--
--   THE COST, MEASURED AND STATED: 9,597 blocks hold more than one
--   submission, and in 349 of them the values differ by more than 5 percent,
--   so at most 349 suppressions could be genuinely distinct deals rather
--   than resubmissions. That is 0.14 percent of deals, it is the price of
--   catching resubmissions at all, and it is auditable rather than hidden,
--   because dedupe never deletes: every suppressed row keeps its lineage.
-- =====================================================================

TRUNCATE int.deal_deduped;

DROP TABLE IF EXISTS tmp_deal_conformed;
CREATE TEMP TABLE tmp_deal_conformed AS
WITH known_broker AS (
    -- One row per broker the directory has EVER named. Membership only, so a
    -- submission quoting a code that was never in the directory is recognised
    -- as unresolvable before any point in time work is attempted.
    SELECT DISTINCT broker_nk FROM stg.broker_directory
), name_match AS (
    -- The name only defect: no property code, just a building name a broker
    -- typed. Resolve it against the register version in force when the deal
    -- was received, which is the same point in time rule the dimension uses.
    SELECT s.source_message_nk, p.property_nk
    FROM stg.deal_submission s
    JOIN LATERAL (
        SELECT r.property_nk
        FROM stg.property_register r
        WHERE r.property_name_norm = s.property_name_norm
          AND r.effective_date <= s.received_date
        ORDER BY r.effective_date DESC, r.raw_line_no DESC
        LIMIT 1
    ) p ON true
    WHERE s.property_nk IS NULL
)
SELECT s.source_message_nk,
       s.deal_nk,
       s.broker_nk,
       COALESCE(s.property_nk, nm.property_nk, 'UNKNOWN')       AS property_nk,
       s.received_ts,
       s.received_date,
       s.deal_value_zar,
       s.source_channel,
       s.is_off_market,
       s.submission_quality,
       kb.broker_nk IS NOT NULL                                 AS broker_is_known,
       -- Built ONCE, here, and read by both consumers below. Writing the
       -- expression out twice would be two chances for the survivors and the
       -- suppression log to disagree about what a duplicate is, and they must
       -- not: DQ-010 divides one by the other.
       concat_ws('|',
           CASE WHEN kb.broker_nk IS NOT NULL THEN s.broker_nk ELSE 'UNKNOWN' END,
           COALESCE(s.property_nk, nm.property_nk, 'UNKNOWN'),
           to_char(s.received_date, 'IYYY-IW'))                 AS deal_fingerprint
FROM stg.deal_submission s
LEFT JOIN name_match   nm ON nm.source_message_nk = s.source_message_nk
LEFT JOIN known_broker kb ON kb.broker_nk = s.broker_nk;

ANALYZE tmp_deal_conformed;

INSERT INTO int.deal_deduped
    (deal_nk, source_message_nk, deal_fingerprint, broker_nk, property_nk,
     sector, received_ts, received_date, deal_value_zar, source_channel,
     is_off_market, submission_quality, broker_sk, property_sk,
     deal_source_sk, loaded_by_run_id)
SELECT d.deal_nk, d.source_message_nk, d.deal_fingerprint, d.broker_nk,
       d.property_nk,
       -- The AUTHORITATIVE sector, read from the property register version in
       -- force at receipt, never from the broker's sector hint. A broker
       -- guessing "Retail" does not rezone a building.
       COALESCE(pd.sector, 'Unknown'),
       d.received_ts, d.received_date, d.deal_value_zar, d.source_channel,
       d.is_off_market, d.submission_quality,
       COALESCE(bd.broker_sk, -1), COALESCE(pd.property_sk, -1),
       COALESCE(ds.deal_source_sk, -1), :run_id
FROM (
    -- DISTINCT ON is the dedupe. The EARLIEST submission in a block is the
    -- original and survives; source_message_nk breaks a tie on the timestamp
    -- so the choice is reproducible rather than plan dependent. It also makes
    -- DQ-003 hold by construction: one surviving row per blocking key.
    SELECT DISTINCT ON (c.deal_fingerprint) c.*
    FROM tmp_deal_conformed c
    ORDER BY c.deal_fingerprint, c.received_ts, c.source_message_nk
) d
-- POINT IN TIME RESOLUTION. The half open range is the whole reason valid_to
-- is 'infinity' and never NULL: one predicate covers the current version and
-- every historical one, and there is no OR ... IS NULL branch to forget.
LEFT JOIN mart.dim_broker bd
       ON bd.broker_nk = d.broker_nk AND d.broker_is_known
      AND d.received_ts >= bd.valid_from AND d.received_ts < bd.valid_to
LEFT JOIN mart.dim_property pd
       ON pd.property_nk = d.property_nk
      AND d.received_ts >= pd.valid_from AND d.received_ts < pd.valid_to
LEFT JOIN mart.dim_deal_source ds
       ON ds.source_channel     = d.source_channel
      AND ds.is_off_market      = d.is_off_market
      AND ds.submission_quality = d.submission_quality;

-- ---------------------------------------------------------------------
-- 9a. THE SUPPRESSED DUPLICATES, WITH LINEAGE.
--
-- Dedupe never deletes. duplicate_of_deal_nk is what makes "which brokers
-- resubmit most, and how long do they wait" answerable, and it is what
-- makes the duplicate rate in DQ-010 auditable instead of asserted.
-- ---------------------------------------------------------------------
INSERT INTO dq.duplicate_submission
    (run_id, source_message_nk, deal_nk, duplicate_of_deal_nk,
     deal_fingerprint, days_after_original, value_delta_pct)
SELECT :run_id, c.source_message_nk, c.deal_nk, k.deal_nk, c.deal_fingerprint,
       round(EXTRACT(EPOCH FROM (c.received_ts - k.received_ts)) / 86400.0, 2),
       CASE WHEN k.deal_value_zar IS NOT NULL AND k.deal_value_zar <> 0
            THEN round(100.0 * (c.deal_value_zar - k.deal_value_zar) / k.deal_value_zar, 4)
       END
FROM tmp_deal_conformed c
-- The join to the SURVIVORS is what identifies a suppression: any conformed
-- row whose blocking key already has a different survivor was suppressed.
-- Deriving it from what actually landed, rather than from a flag set during
-- the insert, means the log cannot drift out of step with the fact.
JOIN int.deal_deduped k ON k.deal_fingerprint = c.deal_fingerprint
WHERE c.source_message_nk <> k.source_message_nk
ON CONFLICT (source_message_nk) DO NOTHING;

ANALYZE int.deal_deduped;


-- =====================================================================
-- STEP 10. int.stage_event_sequenced. WINDOW FUNCTIONS EARN THEIR KEEP.
-- =====================================================================
-- WHAT IS DERIVED HERE AND WHY IT CANNOT BE DERIVED LATER
--   from_stage and days_in_prev_stage are properties of a TRANSITION, and a
--   transition only exists relative to the previous event for the same
--   deal. LAG over (deal, event order) is the whole computation. Doing it
--   in a report instead would mean every report re-running a window over
--   1.1 million rows, and two reports disagreeing the first time one of
--   them ordered the partition slightly differently.
--
-- THE ORDERING TIEBREAKER IS LOAD BEARING
--   ORDER BY event_ts, raw_line_no. Two events on one timestamp must
--   sequence identically on every run, or every dwell measure downstream
--   moves between runs and nobody can reconcile yesterday's report against
--   today's.
--
-- THREE CLASSES OF EVENT NEVER REACH THE FACT
--   ORPHAN_DEAL           an event for a deal reference that never survived
--                         submission, or that was suppressed as a
--                         duplicate. It has no deal to belong to.
--   EVENT_BEFORE_RECEIPT  a capture error dated the event before its own
--                         deal arrived. Excluded BEFORE the window, not
--                         after: sorted by timestamp it would become the
--                         deal's first event and shift from_stage for every
--                         event after it.
--   IMPOSSIBLE_TRANSITION out of a terminal stage, or backwards two or more
--                         stages. Only detectable AFTER the window, because
--                         it is defined relative to the previous event.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 10a. THE TWO CLASSES THAT CAN BE JUDGED WITHOUT A WINDOW.
--
-- Identified by re-reading stg rather than by remembering what the insert
-- below skipped. A set difference against what actually landed cannot
-- drift out of step with the statement that landed it.
-- ---------------------------------------------------------------------
INSERT INTO dq.quarantine_stage_event
    (run_id, reason_code, event_nk, deal_nk, stage_nk, event_ts_text,
     detected_layer, source_file, raw_line_no, raw_payload)
SELECT :run_id,
       CASE WHEN d.deal_nk IS NULL THEN 'ORPHAN_DEAL'
            ELSE 'EVENT_BEFORE_RECEIPT' END,
       e.event_nk, e.deal_nk, e.stage_nk, e.event_ts::text, 'int',
       e.source_file, e.raw_line_no,
       jsonb_build_object('event_nk', e.event_nk, 'deal_nk', e.deal_nk,
                          'event_ts', e.event_ts,
                          'deal_received_ts', d.received_ts)
FROM stg.stage_event e
LEFT JOIN int.deal_deduped d ON d.deal_nk = e.deal_nk
WHERE d.deal_nk IS NULL OR e.event_ts < d.received_ts
ON CONFLICT (source_file, raw_line_no) DO NOTHING;

-- ---------------------------------------------------------------------
-- 10b. THE SEQUENCER, AS A FUNCTION, BECAUSE IT HAS TO RUN MORE THAN ONCE.
--
-- p_deal_nks NULL rebuilds every deal. A non NULL array rebuilds only
-- those deals, which is what makes the fixed point loop below cheap: a
-- pass that removes 1,200 anomalies re-sequences 1,200 deals, not 249,000.
--
-- The eligible set is defined by a NOT EXISTS against the quarantine table
-- rather than by an exclusion list carried in memory. That is what makes
-- the function idempotent and the loop convergent: an event that has been
-- quarantined is invisible to every later pass, permanently, and a re-run
-- of the whole file on a warm database starts from the same exclusion set
-- it finished with.
-- ---------------------------------------------------------------------
CREATE OR REPLACE FUNCTION util.fn_sequence_stage_events(
    p_run_id   bigint,
    p_deal_nks text[] DEFAULT NULL
)
RETURNS bigint
LANGUAGE plpgsql AS $fn$
DECLARE
    v_rows bigint;
BEGIN
    IF p_deal_nks IS NULL THEN
        TRUNCATE int.stage_event_sequenced;
    ELSE
        DELETE FROM int.stage_event_sequenced WHERE deal_nk = ANY (p_deal_nks);
    END IF;

    INSERT INTO int.stage_event_sequenced
        (event_nk, deal_nk, source_message_nk, from_stage_sk, to_stage_sk,
         event_ts, event_date, event_seq, days_in_prev_stage, is_forward_move,
         is_terminal_event, broker_sk, property_sk, deal_source_sk,
         deal_value_zar, loaded_by_run_id)
    WITH eligible AS (
        SELECT e.event_nk, e.deal_nk, e.to_stage_sk, e.event_ts, e.raw_line_no,
               e.stage_value_zar, d.source_message_nk, d.broker_nk,
               d.property_nk, d.deal_source_sk, d.deal_value_zar
        FROM stg.stage_event e
        JOIN int.deal_deduped d ON d.deal_nk = e.deal_nk
        WHERE e.event_ts >= d.received_ts
          AND (p_deal_nks IS NULL OR e.deal_nk = ANY (p_deal_nks))
          AND NOT EXISTS (
              SELECT 1 FROM dq.quarantine_stage_event q
              WHERE q.source_file = e.source_file
                AND q.raw_line_no = e.raw_line_no)
    ), sequenced AS (
        SELECT s.*,
               row_number() OVER w       AS event_seq,
               lag(s.to_stage_sk) OVER w AS prev_stage_sk,
               lag(s.event_ts)    OVER w AS prev_event_ts
        FROM eligible s
        WINDOW w AS (PARTITION BY s.deal_nk ORDER BY s.event_ts, s.raw_line_no)
    )
    SELECT q.event_nk, q.deal_nk, q.source_message_nk,
           -- 0, never NULL. stage_sk 0 is a real dimension member ('Not
           -- applicable'), which keeps the fact's foreign key NOT NULL and
           -- keeps every funnel query from special casing the top of the
           -- funnel.
           COALESCE(q.prev_stage_sk, 0),
           q.to_stage_sk,
           q.event_ts,
           (q.event_ts AT TIME ZONE 'Africa/Johannesburg')::date,
           q.event_seq,
           round(EXTRACT(EPOCH FROM (q.event_ts - q.prev_event_ts)) / 86400.0, 2),
           -- Derived from stage_order in the dimension, never from a
           -- hardcoded list of stage names, so adding a stage does not
           -- silently reclassify history. A first event counts as forward.
           COALESCE(ts.stage_order > fs.stage_order, true),
           ts.is_terminal,
           COALESCE(bd.broker_sk, -1),
           COALESCE(pd.property_sk, -1),
           q.deal_source_sk,
           -- The revised value if this event carried one, otherwise the
           -- deal's current value. NON ADDITIVE across events for one deal,
           -- which is documented on the fact column because that is where
           -- somebody will be tempted to sum it.
           COALESCE(q.stage_value_zar, q.deal_value_zar),
           p_run_id
    FROM sequenced q
    JOIN mart.dim_stage ts ON ts.stage_sk = q.to_stage_sk
    JOIN mart.dim_stage fs ON fs.stage_sk = COALESCE(q.prev_stage_sk, 0)
    -- Resolved at EVENT_TS, not at receipt. A deal that reached Legal after
    -- its broker changed agency must report that event under the new agency
    -- and its earlier events under the old one. That is the entire reason the
    -- dimension is Type 2, and DQ-005 is the rule that proves it held for
    -- every fact row.
    LEFT JOIN mart.dim_broker bd
           ON bd.broker_nk = q.broker_nk
          AND q.event_ts >= bd.valid_from AND q.event_ts < bd.valid_to
    LEFT JOIN mart.dim_property pd
           ON pd.property_nk = q.property_nk
          AND q.event_ts >= pd.valid_from AND q.event_ts < pd.valid_to;

    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END $fn$;
COMMENT ON FUNCTION util.fn_sequence_stage_events(bigint, text[]) IS
'(Re)derives int.stage_event_sequenced from stg for all deals (NULL) or a given set. The eligible set excludes anything already in dq.quarantine_stage_event, which is what makes the anomaly removal loop in 03 converge and makes a re-run a no-op.';

-- ---------------------------------------------------------------------
-- 10c. REMOVE THE IMPOSSIBLE TRANSITIONS. ITERATE TO A FIXED POINT.
--
-- WHY THIS IS A LOOP AND NOT A SINGLE PASS, PROVED RATHER THAN ASSUMED
--   The first version of this step assumed one pass was enough, on the
--   reasoning that every seeded anomaly is the last event of its deal. An
--   assertion was written to check that assumption rather than trust it,
--   and it FAILED on the real data: 1,219 anomalies had a successor. The
--   cause is a second, independent defect. One seeded defect mistypes the
--   year on an event, which throws it to 2031, so it sorts after its deal's
--   real terminal event and the transition INTO it comes out of a terminal
--   stage. Removing it then changes the predecessor of nothing, but
--   removing an anomaly in the MIDDLE of a deal changes the from_stage of
--   the event after it, which can create a brand new anomaly that was
--   invisible on the first pass.
--
--   So the removal has to iterate until nothing changes. This is the
--   difference between a pipeline that is correct and one that happens to
--   be correct on the data the author looked at.
--
-- WHY THE ITERATION CAP EXISTS
--   Each pass removes at least one event, so the loop must terminate. The
--   cap is there so that if a future change ever breaks that argument, the
--   load fails in seconds with a clear message instead of spinning all
--   night.
-- ---------------------------------------------------------------------
SELECT util.fn_sequence_stage_events(:run_id, NULL) AS events_sequenced;

DO $$
DECLARE
    v_run_id   bigint  := current_setting('dealflow.run_id')::bigint;
    v_pass     integer := 0;
    v_max_pass integer := 20;
    v_bad      bigint;
    v_deals    text[];
BEGIN
    LOOP
        v_pass := v_pass + 1;

        WITH bad AS (
            SELECT s.event_nk, s.deal_nk, s.event_ts,
                   CASE WHEN fs.is_terminal THEN 'OUT_OF_TERMINAL_STAGE'
                        ELSE 'BACKWARDS_TWO_OR_MORE' END AS detail
            FROM int.stage_event_sequenced s
            JOIN mart.dim_stage fs ON fs.stage_sk = s.from_stage_sk
            JOIN mart.dim_stage ts ON ts.stage_sk = s.to_stage_sk
            -- One step back is LEGAL and is seeded deliberately:
            -- re-underwriting after due diligence findings is normal
            -- business, and it is what gives reopen_count and
            -- is_forward_move something real to measure.
            WHERE fs.is_terminal OR ts.stage_order <= fs.stage_order - 2
        ), logged AS (
            INSERT INTO dq.quarantine_stage_event
                (run_id, reason_code, event_nk, deal_nk, stage_nk,
                 event_ts_text, detected_layer, source_file, raw_line_no,
                 raw_payload)
            SELECT v_run_id, 'IMPOSSIBLE_TRANSITION', b.event_nk, b.deal_nk,
                   e.stage_nk, b.event_ts::text, 'int', e.source_file,
                   e.raw_line_no,
                   jsonb_build_object('event_nk', b.event_nk,
                                      'deal_nk',  b.deal_nk,
                                      'detail',   b.detail,
                                      'removal_pass', v_pass)
            FROM bad b
            JOIN stg.stage_event e ON e.event_nk = b.event_nk
            ON CONFLICT (source_file, raw_line_no) DO NOTHING
            RETURNING 1
        )
        SELECT count(*), array_agg(DISTINCT b.deal_nk) INTO v_bad, v_deals
        FROM bad b;

        EXIT WHEN v_bad = 0;

        IF v_pass > v_max_pass THEN
            RAISE EXCEPTION
                'Anomaly removal did not converge in % passes (% still failing). Each pass must remove at least one event, so this means an event is being detected as impossible without being excluded from the next pass.',
                v_max_pass, v_bad;
        END IF;

        RAISE NOTICE 'anomaly removal pass %: % events quarantined across % deals',
                     v_pass, v_bad, cardinality(v_deals);

        PERFORM util.fn_sequence_stage_events(v_run_id, v_deals);
    END LOOP;

    RAISE NOTICE 'anomaly removal converged after % passes', v_pass - 1;
END $$;

ANALYZE int.stage_event_sequenced;


-- =====================================================================
-- STEP 11. int.deal_milestone. TEN PIVOTS IN ONE PASS.
-- =====================================================================
-- Aggregate FILTER, not ten correlated subqueries. The subquery form reads
-- the event stream ten times and is the single most common way an
-- accumulating snapshot load ends up slower than the fact it feeds.
--
-- min() rather than max() for every milestone: a reopened deal enters
-- Underwriting twice and the milestone is the FIRST time it got there.
-- Using max() would let a reopen silently rewrite history, and the reopen
-- is counted separately anyway.
-- =====================================================================

TRUNCATE int.deal_milestone;

INSERT INTO int.deal_milestone
    (deal_nk, received_ts, triaged_ts, qualified_ts, underwriting_ts,
     offer_ts, due_diligence_ts, legal_ts, closed_won_ts, lost_ts,
     stalled_ts, stage_count, reopen_count, current_stage_sk, last_event_ts,
     loaded_by_run_id)
SELECT d.deal_nk,
       -- Receipt comes from the SUBMISSION, not from the RECEIVED event. The
       -- submission is the authoritative arrival, and one seeded defect
       -- mistypes the year on an event, which would otherwise move a deal's
       -- receipt to 2031.
       d.received_ts,
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 2),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 3),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 4),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 5),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 6),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 7),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 8),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 9),
       min(s.event_ts) FILTER (WHERE s.to_stage_sk = 10),
       count(DISTINCT s.to_stage_sk),
       -- A backward move is a reopen. is_forward_move already encodes it,
       -- derived from stage_order, so this does not re-decide the question.
       count(*) FILTER (WHERE NOT s.is_forward_move),
       -- The stage the deal is in NOW: the to_stage of its last event.
       -- DISTINCT ON would need a second pass over the same rows, so the
       -- ordered aggregate does it inside this one.
       COALESCE((array_agg(s.to_stage_sk ORDER BY s.event_seq DESC))[1], 1),
       COALESCE(max(s.event_ts), d.received_ts),
       :run_id
FROM int.deal_deduped d
-- LEFT, because a deal whose only event was quarantined is still a received
-- deal: the submission IS the receipt. Dropping it here would make the
-- accumulating snapshot stop reconciling to the submission feed, and an
-- unexplained gap between the two is far worse than a deal sitting at
-- Received with no event history.
LEFT JOIN int.stage_event_sequenced s ON s.deal_nk = d.deal_nk
GROUP BY d.deal_nk, d.received_ts;

ANALYZE int.deal_milestone;


-- =====================================================================
-- STEP 12. THE int DQ GATE
-- =====================================================================
SELECT * FROM util.fn_run_dq_gate('int', :run_id);

\echo ''
\echo '03_load.sql complete. Row counts by layer:'
SELECT 'raw.deal_submission'      AS object, count(*) AS rows FROM raw.deal_submission
UNION ALL SELECT 'raw.stage_event',           count(*) FROM raw.stage_event
UNION ALL SELECT 'stg.deal_submission',       count(*) FROM stg.deal_submission
UNION ALL SELECT 'stg.stage_event',           count(*) FROM stg.stage_event
UNION ALL SELECT 'dq.reject_deal_submission', count(*) FROM dq.reject_deal_submission
UNION ALL SELECT 'dq.quarantine_stage_event', count(*) FROM dq.quarantine_stage_event
UNION ALL SELECT 'dq.duplicate_submission',   count(*) FROM dq.duplicate_submission
UNION ALL SELECT 'mart.dim_agency',           count(*) FROM mart.dim_agency
UNION ALL SELECT 'mart.dim_broker',           count(*) FROM mart.dim_broker
UNION ALL SELECT 'mart.dim_property',         count(*) FROM mart.dim_property
UNION ALL SELECT 'int.deal_deduped',          count(*) FROM int.deal_deduped
UNION ALL SELECT 'int.stage_event_sequenced', count(*) FROM int.stage_event_sequenced
UNION ALL SELECT 'int.deal_milestone',        count(*) FROM int.deal_milestone
ORDER BY 1;
