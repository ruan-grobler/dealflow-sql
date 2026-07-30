-- =====================================================================
-- 04_facts.sql
-- DealFlow warehouse: int to the star, then the gate, then the watermarks.
--
-- WHAT THIS FILE OWNS
--   1. Creating any partition the incoming events need.
--   2. mart.fct_deal_stage_event, the TRANSACTION fact. Append only.
--   3. mart.fct_deal_pipeline, the ACCUMULATING SNAPSHOT fact. Updated in
--      place, with every overwrite logged to dq.restatement_log.
--   4. The materialized aggregate the monthly reporting layer reads.
--   5. The mart DQ gate.
--   6. Advancing the watermarks and closing meta.load_run, IN THAT ORDER
--      and only after the gate has passed.
--
-- THE TWO FACTS ARE DELIBERATELY DIFFERENT ANIMALS
--   The event fact is the IMMUTABLE AUDIT TRAIL. Rows are appended and
--   never updated, so the history of what the pipeline system told us can
--   always be replayed.
--   The pipeline fact is CURRENT TRUTH. Rows are overwritten as milestones
--   land, which is the only way "days from receipt to offer" can be one
--   indexed scan instead of a window function over 1.1 million rows.
--   Keeping both is not redundancy. Keeping only the snapshot would lose
--   the history; keeping only the events would make every cycle time
--   question expensive.
--
-- WHY THE WATERMARKS MOVE LAST
--   A watermark that advances before the gate passes turns a failed run
--   into permanently skipped data, and nobody finds out until a month end
--   number is wrong. ON_ERROR_STOP plus a gate that RAISES means a failed
--   run leaves the watermark where it was and the next run reprocesses the
--   same batch. Reprocessing is safe because every write in 03 and 04 is
--   idempotent, which is what earns the right to do it this way.
-- =====================================================================

\set ON_ERROR_STOP on
\timing on

SET timezone = 'Africa/Johannesburg';

-- The run opened by 03. Reusing it rather than opening a second one keeps
-- one row per pipeline execution, which is what makes meta.load_run
-- readable as a history of runs instead of a history of scripts.
SELECT max(run_id) AS run_id FROM meta.load_run WHERE status = 'RUNNING'
\gset

-- Published as a session GUC so the plpgsql blocks below can read it. A psql
-- variable is substituted by the client and is therefore invisible inside a
-- DO block body, which is a single string literal by the time the server
-- sees it.
SELECT set_config('dealflow.run_id', :'run_id', false) AS run_id_published;

\echo 'continuing load run' :run_id


-- =====================================================================
-- STEP 1. MAKE SURE EVERY MONTH THE DATA TOUCHES HAS A PARTITION
-- =====================================================================
-- The load asks for the partitions it is about to need rather than
-- assuming the DDL in 02 covered them. That is what lets this pipeline run
-- unattended in January 2027 without a human editing DDL first.
--
-- Events outside the declared range still have somewhere to land: the
-- DEFAULT partition. A month maker plus a default partition is a
-- deliberate landing zone with an alarm on it (DQ-012), which beats a load
-- that aborts at 3am on one mistyped year.
-- =====================================================================
DO $$
DECLARE
    r      record;
    v_made integer := 0;
BEGIN
    FOR r IN
        SELECT DISTINCT date_trunc('month', event_date)::date AS m
        FROM int.stage_event_sequenced
        -- Outside this window the month maker would create partitions for
        -- typos. 2031 has no business being a partition; it has business
        -- being in the DEFAULT partition where DQ-012 can count it.
        WHERE event_date >= DATE '2020-01-01' AND event_date < DATE '2027-01-01'
        ORDER BY 1
    LOOP
        PERFORM util.fn_ensure_month_partition(r.m, current_setting('dealflow.run_id', true)::bigint);
        v_made := v_made + 1;
    END LOOP;
    RAISE NOTICE 'partitions ensured for % months', v_made;
END $$;


-- =====================================================================
-- STEP 2. mart.fct_deal_stage_event. THE TRANSACTION FACT.
-- =====================================================================
-- IDEMPOTENCE IS ON CONFLICT AGAINST uq_fct_event_nk, NOT AN ANTI JOIN.
--   event_nk is the source system's own event id, so a replay offers the
--   same keys and the unique index refuses them. One index probe per row
--   during the insert, against a semi join that would have to probe all 85
--   partitions for every one of 1.1 million rows before inserting any of
--   them. Same guarantee, and the guarantee is physical rather than
--   procedural: it holds even if a future script forgets the anti join.
--
--   The conflict target has to include event_date because PostgreSQL
--   requires the partition key in every unique index on a partitioned
--   table. That is a constraint of the storage model, not a modelling
--   choice, and it is the same reason PRIMARY KEY (deal_event_sk,
--   event_date) reads oddly.
-- =====================================================================
INSERT INTO mart.fct_deal_stage_event
    (event_nk, deal_nk, source_message_nk, broker_sk, property_sk,
     deal_source_sk, from_stage_sk, to_stage_sk, event_ts, event_date,
     deal_value_zar, days_in_prev_stage, is_forward_move, is_terminal_event,
     loaded_by_run_id)
SELECT s.event_nk, s.deal_nk, s.source_message_nk, s.broker_sk, s.property_sk,
       s.deal_source_sk, s.from_stage_sk, s.to_stage_sk, s.event_ts,
       s.event_date, s.deal_value_zar, s.days_in_prev_stage,
       s.is_forward_move, s.is_terminal_event, :run_id
FROM int.stage_event_sequenced s
ON CONFLICT (event_nk, event_date) DO NOTHING;

-- The DEFAULT partition and any partition that predates the statistics
-- policy get their extended statistics object here. DQ-008 fails the run if
-- a populated partition is missing one, and this is the fix for that failure
-- rather than a manual step in a runbook.
SELECT util.fn_refresh_partition_stats() AS stats_objects_created;

-- ANALYZE the PARENT, which analyzes every partition and populates the
-- per partition extended statistics. Statistics on the parent alone were
-- measured to leave per partition estimates byte identical, which is why
-- the objects live on the children. See the note in 02.
ANALYZE mart.fct_deal_stage_event;


-- =====================================================================
-- STEP 3. mart.fct_deal_pipeline. THE ACCUMULATING SNAPSHOT.
-- =====================================================================
-- The row is assembled once, in a temp table, because three different
-- statements need it: the restatement log needs it next to the OLD values,
-- the MERGE needs it as its source, and the run log needs its cardinality.
-- Re-deriving it three times would be three chances for the three
-- statements to disagree.
-- =====================================================================
DROP TABLE IF EXISTS tmp_pipeline_target;
CREATE TEMP TABLE tmp_pipeline_target AS
SELECT d.deal_nk,
       d.source_message_nk,
       d.broker_sk,
       d.property_sk,
       d.deal_source_sk,
       d.received_date,
       d.received_ts,
       m.triaged_ts, m.qualified_ts, m.underwriting_ts, m.offer_ts,
       m.due_diligence_ts, m.legal_ts, m.closed_won_ts, m.lost_ts,
       m.stalled_ts,
       -- The lag measures, precomputed. They are the whole reason an
       -- accumulating snapshot is worth maintaining: without them this table
       -- is just a slower copy of the event fact.
       round(EXTRACT(EPOCH FROM (m.qualified_ts - d.received_ts)) / 86400.0, 2) AS days_to_qualify,
       round(EXTRACT(EPOCH FROM (m.offer_ts     - d.received_ts)) / 86400.0, 2) AS days_to_offer,
       round(EXTRACT(EPOCH FROM (LEAST(m.closed_won_ts, m.lost_ts, m.stalled_ts)
                                 - d.received_ts)) / 86400.0, 2)               AS days_to_terminal,
       m.stage_count,
       m.reopen_count,
       m.current_stage_sk,
       -- Every state flag comes from dim_stage, so "what counts as lost" is
       -- defined in exactly one place. The CHECK constraint on the fact then
       -- guarantees the four flags stay mutually exclusive, and the matching
       -- CHECK on dim_stage guarantees they can be.
       NOT st.is_terminal AS is_open,
       st.is_won,
       st.is_lost,
       st.is_stalled,
       d.deal_value_zar,
       -- The value as first submitted, kept alongside the current value so a
       -- restatement is MEASURABLE and not merely logged.
       d.deal_value_zar AS first_value_zar
FROM int.deal_deduped d
JOIN int.deal_milestone m ON m.deal_nk = d.deal_nk
JOIN mart.dim_stage st    ON st.stage_sk = m.current_stage_sk;

ANALYZE tmp_pipeline_target;

-- ---------------------------------------------------------------------
-- 3a. LOG THE RESTATEMENTS BEFORE OVERWRITING THEM.
--
-- This table is CURRENT TRUTH and is overwritten in place, so it is the
-- one place in the warehouse where information can be destroyed. The log
-- is the only record that the number used to be something else, and it has
-- to be written before the MERGE because afterwards the old value is gone.
-- ---------------------------------------------------------------------
INSERT INTO dq.restatement_log (run_id, deal_nk, attribute_name, old_value, new_value)
SELECT :run_id, p.deal_nk, 'deal_value_zar', p.deal_value_zar::text, t.deal_value_zar::text
FROM mart.fct_deal_pipeline p
JOIN tmp_pipeline_target t ON t.deal_nk = p.deal_nk
WHERE p.deal_value_zar IS DISTINCT FROM t.deal_value_zar
UNION ALL
SELECT :run_id, p.deal_nk, 'received_date', p.received_date::text, t.received_date::text
FROM mart.fct_deal_pipeline p
JOIN tmp_pipeline_target t ON t.deal_nk = p.deal_nk
WHERE p.received_date IS DISTINCT FROM t.received_date;

MERGE INTO mart.fct_deal_pipeline p
USING tmp_pipeline_target t ON p.deal_nk = t.deal_nk
WHEN MATCHED THEN UPDATE SET
    source_message_nk = t.source_message_nk,
    broker_sk = t.broker_sk, property_sk = t.property_sk,
    deal_source_sk = t.deal_source_sk,
    received_date = t.received_date, received_ts = t.received_ts,
    triaged_ts = t.triaged_ts, qualified_ts = t.qualified_ts,
    underwriting_ts = t.underwriting_ts, offer_ts = t.offer_ts,
    due_diligence_ts = t.due_diligence_ts, legal_ts = t.legal_ts,
    closed_won_ts = t.closed_won_ts, lost_ts = t.lost_ts,
    stalled_ts = t.stalled_ts,
    days_to_qualify = t.days_to_qualify, days_to_offer = t.days_to_offer,
    days_to_terminal = t.days_to_terminal,
    stage_count = t.stage_count, reopen_count = t.reopen_count,
    current_stage_sk = t.current_stage_sk,
    is_open = t.is_open, is_won = t.is_won, is_lost = t.is_lost,
    is_stalled = t.is_stalled,
    deal_value_zar = t.deal_value_zar,
    -- first_value_zar is NOT touched on update. That is the point of it.
    value_revision_count = p.value_revision_count
        + CASE WHEN p.deal_value_zar IS DISTINCT FROM t.deal_value_zar THEN 1 ELSE 0 END,
    last_updated_at = clock_timestamp(),
    loaded_by_run_id = :run_id
WHEN NOT MATCHED THEN INSERT
    (deal_nk, source_message_nk, broker_sk, property_sk, deal_source_sk,
     received_date, received_ts, triaged_ts, qualified_ts, underwriting_ts,
     offer_ts, due_diligence_ts, legal_ts, closed_won_ts, lost_ts, stalled_ts,
     days_to_qualify, days_to_offer, days_to_terminal, stage_count,
     reopen_count, current_stage_sk, is_open, is_won, is_lost, is_stalled,
     deal_value_zar, first_value_zar, value_revision_count, loaded_by_run_id)
VALUES
    (t.deal_nk, t.source_message_nk, t.broker_sk, t.property_sk,
     t.deal_source_sk, t.received_date, t.received_ts, t.triaged_ts,
     t.qualified_ts, t.underwriting_ts, t.offer_ts, t.due_diligence_ts,
     t.legal_ts, t.closed_won_ts, t.lost_ts, t.stalled_ts, t.days_to_qualify,
     t.days_to_offer, t.days_to_terminal, t.stage_count, t.reopen_count,
     t.current_stage_sk, t.is_open, t.is_won, t.is_lost, t.is_stalled,
     t.deal_value_zar, t.first_value_zar, 0, :run_id);

ANALYZE mart.fct_deal_pipeline;


-- =====================================================================
-- STEP 4. THE MATERIALIZED AGGREGATE
-- =====================================================================
-- mart.mv_broker_month. One row per broker per month, DENSE.
--
-- WHY DENSE, AND WHY THAT IS THE ENTIRE POINT
--   A GROUP BY over the fact returns a row only for the broker-months that
--   had activity. Feed that to a moving average, a month over month delta
--   or a chart and the gaps close up silently: a broker who did nothing in
--   May gets April compared against June and the report says they were
--   flat when they were absent. Cross joining the broker list against the
--   month spine and LEFT JOINing the aggregate produces the zero rows, so
--   a zero month is a zero and not a missing row.
--
-- WHY MATERIALIZED AND NOT A VIEW
--   It aggregates 1.1 million fact rows into about 60,000. A dashboard
--   that reads it a hundred times a day should pay that cost once per
--   load, not a hundred times.
--
-- WHY THE UNIQUE INDEX EXISTS
--   REFRESH MATERIALIZED VIEW CONCURRENTLY requires one, and without
--   CONCURRENTLY the refresh takes an ACCESS EXCLUSIVE lock that blocks
--   every reader for its duration. On a warehouse that anyone queries
--   during the load window that is the difference between a refresh and an
--   outage.
-- =====================================================================
DROP MATERIALIZED VIEW IF EXISTS mart.mv_broker_month;
CREATE MATERIALIZED VIEW mart.mv_broker_month AS
WITH month_spine AS (
    -- Distinct months from dim_date, bounded by the facts that exist. Reading
    -- the spine from the date dimension rather than generate_series() is what
    -- keeps the calendar definition in one place.
    SELECT DISTINCT d.month_start_date
    FROM mart.dim_date d
    WHERE d.month_start_date BETWEEN (SELECT min(received_date) FROM mart.fct_deal_pipeline)
                                AND (SELECT max(received_date) FROM mart.fct_deal_pipeline)
), broker AS (
    SELECT b.broker_sk, b.broker_nk, b.full_name, b.agency_name, b.broker_tier
    FROM mart.dim_broker b
    WHERE b.is_current
), grid AS (
    SELECT b.broker_sk, b.broker_nk, b.full_name, b.agency_name, b.broker_tier,
           m.month_start_date
    FROM broker b CROSS JOIN month_spine m
), activity AS (
    SELECT f.broker_sk,
           date_trunc('month', f.event_date)::date            AS month_start_date,
           count(*)                                           AS stage_events,
           count(DISTINCT f.deal_nk)                          AS deals_touched,
           count(*) FILTER (WHERE f.is_terminal_event
                              AND st.is_won)                  AS deals_won,
           sum(f.deal_value_zar) FILTER (WHERE f.is_terminal_event
                                           AND st.is_won)     AS won_value_zar,
           round(avg(f.days_in_prev_stage), 2)                AS avg_days_in_prev_stage
    FROM mart.fct_deal_stage_event f
    JOIN mart.dim_stage st ON st.stage_sk = f.to_stage_sk
    GROUP BY f.broker_sk, date_trunc('month', f.event_date)::date
)
SELECT g.broker_sk, g.broker_nk, g.full_name, g.agency_name, g.broker_tier,
       g.month_start_date,
       COALESCE(a.stage_events, 0)  AS stage_events,
       COALESCE(a.deals_touched, 0) AS deals_touched,
       COALESCE(a.deals_won, 0)     AS deals_won,
       COALESCE(a.won_value_zar, 0) AS won_value_zar,
       a.avg_days_in_prev_stage,
       -- A zero month is a real zero here, so this flag is honest. On a sparse
       -- aggregate it could not be computed at all.
       a.broker_sk IS NULL          AS is_inactive_month
FROM grid g
LEFT JOIN activity a
       ON a.broker_sk = g.broker_sk AND a.month_start_date = g.month_start_date;

COMMENT ON MATERIALIZED VIEW mart.mv_broker_month IS
'One row per current broker per month in the fact window, DENSE. Zero activity months are present as zeros so a moving average or a month over month delta cannot silently close a gap.';

CREATE UNIQUE INDEX uq_mv_broker_month ON mart.mv_broker_month (broker_sk, month_start_date);
ANALYZE mart.mv_broker_month;


-- =====================================================================
-- STEP 5. THE mart DQ GATE. THE LAST THING BEFORE THE WATERMARKS MOVE.
-- =====================================================================
-- Six error rules and one warn rule run here. The error rules include the
-- two that no foreign key can express: DQ-005, that every surrogate key
-- points at the dimension version that was in force when the event
-- happened, and DQ-004, that no deal exists in the event fact without a
-- row in the snapshot. If either fails this file stops and the watermarks
-- stay where they are.
-- =====================================================================
SELECT * FROM util.fn_run_dq_gate('mart', :run_id);


-- =====================================================================
-- STEP 6. ADVANCE THE WATERMARKS
-- =====================================================================
-- Four INGEST watermarks move to the high water mark of what was actually
-- read. Two SCD_APPLY watermarks move to the last dimension snapshot
-- folded into a Type 2 dimension, which is a DATE promoted to a
-- timestamptz rather than an ingest time, because the question the
-- dimension asks is "which snapshots have I already versioned".
-- =====================================================================
UPDATE meta.load_watermark w
SET watermark_ts = v.hwm, last_run_id = :run_id, updated_at = clock_timestamp()
FROM (
    SELECT 'raw.deal_submission'   AS stream_nk, max(ingested_at) AS hwm FROM raw.deal_submission
    UNION ALL SELECT 'raw.stage_event',          max(ingested_at) FROM raw.stage_event
    UNION ALL SELECT 'raw.broker_directory',     max(ingested_at) FROM raw.broker_directory
    UNION ALL SELECT 'raw.property_register',    max(ingested_at) FROM raw.property_register
    UNION ALL SELECT 'mart.dim_broker',          max(snapshot_date)::timestamptz FROM stg.broker_directory
    UNION ALL SELECT 'mart.dim_property',        max(effective_date)::timestamptz FROM stg.property_register
) v
WHERE w.stream_nk = v.stream_nk
  AND v.hwm IS NOT NULL
  -- Never move a watermark backwards. If this run read nothing, the previous
  -- high water mark is still the truth.
  AND v.hwm > w.watermark_ts;


-- =====================================================================
-- STEP 7. CLOSE THE RUN
-- =====================================================================
-- Every count comes from the database, none from a variable this script
-- has been carrying around. A run log that reports what the loader
-- BELIEVED it did is worth nothing during an incident.
-- =====================================================================
UPDATE meta.load_run r
SET status               = 'SUCCESS',
    finished_at          = clock_timestamp(),
    duration_ms          = EXTRACT(EPOCH FROM (clock_timestamp() - r.started_at)) * 1000,
    rows_raw_available   = (SELECT count(*) FROM raw.deal_submission)
                         + (SELECT count(*) FROM raw.stage_event),
    rows_staged          = (SELECT count(*) FROM stg.deal_submission)
                         + (SELECT count(*) FROM stg.stage_event),
    rows_rejected        = (SELECT count(*) FROM dq.reject_deal_submission WHERE run_id = r.run_id),
    rows_quarantined     = (SELECT count(*) FROM dq.quarantine_stage_event WHERE run_id = r.run_id),
    rows_deduped_out     = (SELECT count(*) FROM dq.duplicate_submission   WHERE run_id = r.run_id),
    rows_dim_versioned   = (SELECT count(*) FROM mart.dim_broker   WHERE broker_sk   <> -1)
                         + (SELECT count(*) FROM mart.dim_property WHERE property_sk <> -1),
    rows_fact_inserted   = (SELECT count(*) FROM mart.fct_deal_stage_event WHERE loaded_by_run_id = r.run_id),
    rows_pipeline_upsert = (SELECT count(*) FROM mart.fct_deal_pipeline    WHERE loaded_by_run_id = r.run_id),
    dq_error_failures    = (SELECT count(*) FROM dq.check_result
                            WHERE run_id = r.run_id AND severity = 'error' AND NOT is_pass),
    dq_warn_failures     = (SELECT count(*) FROM dq.check_result
                            WHERE run_id = r.run_id AND severity = 'warn'  AND NOT is_pass)
WHERE r.run_id = :run_id;

-- The partition registry is the retention job's view of the world, so it
-- carries the sizes rather than making that job read pg_class itself.
UPDATE meta.partition_registry pr
SET row_count  = s.n_live_tup,
    size_bytes = pg_total_relation_size(('mart.' || quote_ident(pr.partition_name))::regclass)
FROM pg_stat_user_tables s
WHERE s.schemaname = 'mart' AND s.relname = pr.partition_name;

\echo ''
\echo '04_facts.sql complete. The star:'
SELECT 'mart.fct_deal_stage_event'          AS object, count(*) AS rows FROM mart.fct_deal_stage_event
UNION ALL SELECT 'mart.fct_deal_stage_event_default', count(*) FROM mart.fct_deal_stage_event_default
UNION ALL SELECT 'mart.fct_deal_pipeline',            count(*) FROM mart.fct_deal_pipeline
UNION ALL SELECT 'mart.mv_broker_month',              count(*) FROM mart.mv_broker_month
UNION ALL SELECT 'mart.dim_broker (current)',         count(*) FROM mart.dim_broker   WHERE is_current
UNION ALL SELECT 'mart.dim_broker (all versions)',    count(*) FROM mart.dim_broker
UNION ALL SELECT 'mart.dim_property (current)',       count(*) FROM mart.dim_property WHERE is_current
UNION ALL SELECT 'mart.dim_property (all versions)',  count(*) FROM mart.dim_property
UNION ALL SELECT 'mart.dim_agency',                   count(*) FROM mart.dim_agency
ORDER BY 1;

\echo ''
\echo 'Data quality gate results for this run:'
SELECT r.check_code, d.layer, r.severity, r.observed_value,
       r.threshold_min, r.threshold_max, r.is_pass, d.check_name
FROM dq.check_result r
JOIN dq.check_definition d ON d.check_code = r.check_code
WHERE r.run_id = :run_id
ORDER BY r.check_code;
