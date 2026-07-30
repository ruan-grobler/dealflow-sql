-- =====================================================================
-- 05_analytics.sql
-- DealFlow warehouse: the analytics layer.
--
-- Fourteen questions a commercial property business actually asks, ordered
-- from foundational to advanced. Every query runs against the star built by
-- 01 through 04 and every number in the README was captured from a real run,
-- never transcribed.
--
-- Reading conventions used throughout:
--   * All monetary output is South African Rand. Values are divided by 1e6
--     (_zar_m) or 1e9 (_zar_bn) purely for legibility; the stored measure is
--     numeric(18,2) in whole Rand.
--   * The reporting timezone is Africa/Johannesburg, pinned on the database
--     itself rather than described in a README. See docs/ASSUMPTIONS.md, which
--     also records that no event in this dataset falls in the 00:00 to 01:59
--     JHB window where a UTC reading would land on the previous date, so the
--     boundary is correct by construction and never exercised by the data.
--   * "As at" dates are pinned to the last event in the warehouse, never to
--     now(), so a query re-run next month returns the same answer.
--   * Deal counts come from mart.fct_deal_pipeline (one row per deal).
--     Stage counts come from mart.fct_deal_stage_event (one row per
--     transition). Mixing the two is how a funnel dashboard starts lying, so
--     each query states which grain it is working at.
-- =====================================================================

\timing on

-- =====================================================================
-- Q01. DEAL FLOW BY YEAR
--
-- BUSINESS QUESTION: Is deal flow actually growing, how much value is coming
--   through the door each year, and how much of the year do we lose to the
--   December to January shutdown?
-- TECHNIQUE: The foundational star join. One fact, one conformed dimension,
--   GROUP BY the dimension attribute rather than by a date_trunc on the fact.
--   Joining dim_date instead of computing EXTRACT(YEAR FROM received_date) is
--   what lets the same query pivot to fiscal year or to the shutdown flag
--   without touching the fact table, and is_summer_shutdown is a business
--   definition that belongs in the dimension where everyone shares it.
-- =====================================================================
SELECT d.year_num,
       count(*)                                                        AS deals_received,
       round(sum(p.deal_value_zar) / 1e9, 2)                           AS value_received_zar_bn,
       round(avg(p.deal_value_zar) / 1e6, 2)                           AS avg_deal_zar_m,
       count(*) FILTER (WHERE d.is_summer_shutdown)                    AS deals_in_shutdown,
       round(100.0 * count(*) FILTER (WHERE d.is_summer_shutdown)
             / count(*), 1)                                            AS pct_in_shutdown,
       count(*) FILTER (WHERE p.is_won)                                AS deals_won,
       round(100.0 * count(*) FILTER (WHERE p.is_won) / count(*), 1)   AS win_rate_pct
FROM mart.fct_deal_pipeline p
JOIN mart.dim_date d ON d.date_actual = p.received_date
GROUP BY d.year_num
ORDER BY d.year_num;


-- =====================================================================
-- Q02. DEAL FUNNEL WITH TIME IN STAGE
--
-- BUSINESS QUESTION: Where in the pipeline do deals die, and how long does a
--   deal sit in each stage before it moves?
-- TECHNIQUE: Two different grains resolved in one answer, plus PERCENTILE_CONT
--   rather than AVG for the central tendency.
--
--   WHY PERCENTILE_CONT AND NOT AVG: time in stage is right skewed. A handful
--   of deals sit in Legal for a year and drag the mean far above anything a
--   deal desk would recognise. The median is the number a broker can plan
--   against, and shipping the mean alongside it makes the skew visible instead
--   of hiding it. The measured gap is real: Legal has a median of 26.11 days
--   against a mean of 43.18, so the mean overstates the typical deal by about
--   two thirds.
--
--   WHICH STAGES CAN HAVE A DWELL TIME AT ALL. Because dwell is attributed by
--   from_stage_sk (see below), a terminal stage has none: no event ever LEAVES
--   Closed won, Lost or Stalled, so median_days_in_stage is correctly NULL on
--   those three rows. Quoting a Closed won dwell time would be quoting a
--   statistic this query provably cannot produce, which is why the example
--   above is Legal.
--
--   WHAT THE FUNNEL BASE IS. pct_of_all_received is a share of the deals that
--   recorded a Received event, which is 248,189. Q01 reports 248,940 deals
--   because it counts mart.fct_deal_pipeline, the deal grain. The 751 deal gap
--   is deals whose Received event was quarantined or never arrived at all, and
--   the two numbers are different on purpose rather than by accident: this
--   query is answering an event question and Q01 is answering a deal question.
--
--   WHY TWO GROUPINGS: days_in_prev_stage on an event ENTERING stage N is the
--   time the deal spent in stage N minus 1. So "deals that reached a stage"
--   groups by to_stage_sk, while "time spent in a stage" groups by
--   from_stage_sk. Getting this backwards shifts every dwell time by one stage,
--   which is the single easiest way to publish a wrong funnel.
-- =====================================================================
WITH entered AS (
    -- Grain: one row per stage. COUNT DISTINCT because a reopened deal enters
    -- the same stage more than once and must still count as one deal reached.
    SELECT f.to_stage_sk                AS stage_sk,
           count(DISTINCT f.deal_nk)    AS deals_entered
    FROM mart.fct_deal_stage_event f
    GROUP BY f.to_stage_sk
),
dwell AS (
    -- Attributed by from_stage_sk, per the note above. from_stage_sk = 0 is the
    -- synthetic "Not applicable" member on a deal's first event, which has no
    -- previous stage to measure.
    SELECT f.from_stage_sk AS stage_sk,
           percentile_cont(0.5) WITHIN GROUP (ORDER BY f.days_in_prev_stage) AS p50_days,
           percentile_cont(0.9) WITHIN GROUP (ORDER BY f.days_in_prev_stage) AS p90_days,
           avg(f.days_in_prev_stage)                                         AS mean_days
    FROM mart.fct_deal_stage_event f
    WHERE f.from_stage_sk <> 0
    GROUP BY f.from_stage_sk
)
SELECT s.stage_order,
       s.stage_name,
       s.stage_group,
       e.deals_entered,
       -- Conversion is only meaningful along the sequential part of the funnel.
       -- Closed won, Lost and Stalled are three ALTERNATIVE terminal outcomes,
       -- not a fourth, fifth and sixth step, so a stage to stage conversion into
       -- them would be arithmetic with no meaning. It is returned as NULL and the
       -- share of all received deals is given instead.
       CASE WHEN NOT s.is_terminal
            THEN round(100.0 * e.deals_entered
                       / lag(e.deals_entered) OVER (ORDER BY s.stage_order), 1)
       END                                                        AS conv_from_prior_stage_pct,
       round(100.0 * e.deals_entered
             / first_value(e.deals_entered) OVER (ORDER BY s.stage_order), 1)
                                                                  AS pct_of_all_received,
       round(d.p50_days::numeric, 2)                              AS median_days_in_stage,
       round(d.p90_days::numeric, 2)                              AS p90_days_in_stage,
       round(d.mean_days, 2)                                      AS mean_days_in_stage,
       s.target_days_in_stage                                     AS sla_days,
       CASE WHEN d.p50_days > s.target_days_in_stage THEN 'over SLA' END AS sla_flag
FROM mart.dim_stage s
JOIN entered e      ON e.stage_sk = s.stage_sk
LEFT JOIN dwell d   ON d.stage_sk = s.stage_sk
WHERE s.stage_sk > 0
ORDER BY s.stage_order;


-- =====================================================================
-- Q03. QUARTER ON QUARTER PIPELINE MOVEMENT
--
-- BUSINESS QUESTION: How did submitted volume and value move against the
--   previous quarter, and against the same quarter a year earlier?
-- TECHNIQUE: LAG over an ordered quarterly series, with NULLIF guarding the
--   denominator.
--
--   NULL SAFETY, TWO SEPARATE PROBLEMS: the first period has no predecessor,
--   so LAG returns NULL and the percentage is correctly NULL rather than zero
--   (reporting 0% growth for a period with no baseline is a lie, not a
--   simplification). Separately, a prior period of zero would raise a division
--   by zero, so the denominator is wrapped in NULLIF(x, 0). Wrapping the
--   denominator, not the numerator, is what keeps the arithmetic honest.
--
--   LAG(x, 4) gives the year on year comparison off the same window, which is
--   cheaper and clearer than self joining the series to itself.
-- =====================================================================
WITH quarterly AS (
    SELECT d.year_num,
           d.quarter_num,
           count(*)               AS deals,
           sum(p.deal_value_zar)  AS value_zar
    FROM mart.fct_deal_pipeline p
    JOIN mart.dim_date d ON d.date_actual = p.received_date
    GROUP BY d.year_num, d.quarter_num
),
movement AS (
    SELECT q.*,
           lag(q.deals)     OVER w AS prev_q_deals,
           lag(q.value_zar) OVER w AS prev_q_value,
           lag(q.deals, 4)  OVER w AS prior_year_deals
    FROM quarterly q
    WINDOW w AS (ORDER BY q.year_num, q.quarter_num)
)
SELECT m.year_num,
       m.quarter_num,
       m.deals,
       round(m.value_zar / 1e9, 2)                                       AS value_zar_bn,
       m.prev_q_deals,
       round(100.0 * (m.deals - m.prev_q_deals)
             / NULLIF(m.prev_q_deals, 0), 1)                             AS deals_qoq_pct,
       round(100.0 * (m.value_zar - m.prev_q_value)
             / NULLIF(m.prev_q_value, 0), 1)                             AS value_qoq_pct,
       round(100.0 * (m.deals - m.prior_year_deals)
             / NULLIF(m.prior_year_deals, 0), 1)                         AS deals_yoy_pct,
       CASE WHEN m.prev_q_deals IS NULL THEN 'no prior period' END       AS baseline_note
FROM movement m
ORDER BY m.year_num, m.quarter_num;


-- =====================================================================
-- Q04. TOP CLOSER MOMENTUM: RUNNING TOTAL AND MOVING AVERAGE
--
-- BUSINESS QUESTION: For our single highest earning broker, what is their
--   cumulative won value over time and what is the smoothed three month trend?
-- TECHNIQUE: Two window frames on one ordered series, chosen deliberately.
--
--   ROWS VERSUS RANGE, THE ACTUAL DIFFERENCE: ROWS counts PHYSICAL ROWS either
--   side of the current row. RANGE counts LOGICAL VALUES of the ORDER BY
--   expression, so with an interval offset it counts CALENDAR DISTANCE.
--
--   ROWS BETWEEN 2 PRECEDING averages the last three ROWS THAT EXIST, happily
--   reaching back across a six month gap and calling the result a three month
--   average. RANGE BETWEEN INTERVAL '2 months' PRECEDING averages only what
--   genuinely falls inside the three calendar month window, and returns the
--   current month alone when the two before it were silent. The rows_in_frame
--   and range_in_frame columns expose the two frame sizes side by side.
--
--   HONEST NOTE ON WHAT THIS DATA ACTUALLY SHOWS. On this warehouse the top
--   broker's won series is completely dense: 76 rows across the 76 calendar
--   months from 2020-04 to 2026-07, with no silent month anywhere. So
--   rows_in_frame and range_in_frame agree on every row and the divergence this
--   query exists to demonstrate is NOT visible here. That is the honest
--   reading, and it is more useful than a staged gap. The distinction still
--   decides the code: the moment this runs against a thinner book, or against a
--   broker outside the top of the leaderboard, ROWS starts quietly averaging
--   across gaps and RANGE does not, and by then the report is already
--   published. The frame is chosen for the case that breaks, not the case that
--   happens to be dense today.
--
--   RULE OF THUMB: a cumulative total wants ROWS UNBOUNDED PRECEDING, because
--   "everything up to here" is a row concept. Any time based average wants
--   RANGE with an interval, because "the last three months" is a calendar
--   concept and gaps must not be silently skipped.
-- =====================================================================
WITH top_broker AS (
    -- Chosen by measured won value rather than hardcoded, so the query survives
    -- a reload of the warehouse.
    SELECT p.broker_sk
    FROM mart.fct_deal_pipeline p
    WHERE p.is_won
    GROUP BY p.broker_sk
    -- NULLS LAST IS NOT DECORATION, IT IS THE FIX FOR A BUG THIS QUERY HAD.
    -- PostgreSQL defaults DESC to NULLS FIRST. About 1.2 percent of submissions
    -- arrive with no value at all, so a broker whose only won deal has a NULL
    -- value sums to NULL and sorts ABOVE the genuine top earner. This query
    -- returned exactly one row for such a broker before the fix, and it looked
    -- like an empty result rather than a wrong one, which is why it survived a
    -- read through. Any "top N by an aggregate that can be NULL" needs this.
    ORDER BY sum(p.deal_value_zar) DESC NULLS LAST, p.broker_sk
    LIMIT 1
),
monthly AS (
    -- BOUNDED TO THE WAREHOUSE HORIZON ON PURPOSE. The generator injects a
    -- small number of deliberately out of range close dates (25 won deals land
    -- in 2031), which ASSERT-STG-018 and DQ-012 both catch and report. They
    -- still reach the fact, where 04_facts.sql parks them in the DEFAULT
    -- partition. Without this bound they also reach THIS report, and a manager
    -- reads that the top broker closed R16.81m in September 2031. Known bad
    -- data must not be allowed to walk into a business answer just because it
    -- was correctly flagged somewhere upstream, so the horizon that bounds the
    -- partition range (04_facts.sql) bounds the report too.
    SELECT date_trunc('month', p.closed_won_ts)::date AS won_month,
           count(*)                                  AS deals_won,
           sum(p.deal_value_zar)                     AS won_value_zar
    FROM mart.fct_deal_pipeline p
    JOIN top_broker t ON t.broker_sk = p.broker_sk
    WHERE p.is_won
      AND p.closed_won_ts < DATE '2027-01-01'
    GROUP BY 1
)
SELECT m.won_month,
       m.deals_won,
       round(m.won_value_zar / 1e6, 2)                                    AS won_zar_m,
       round(sum(m.won_value_zar) OVER (ORDER BY m.won_month
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 1e6, 2)
                                                                          AS cumulative_won_zar_m,
       round(avg(m.won_value_zar) OVER (ORDER BY m.won_month
                 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) / 1e6, 2)      AS ma3_rows_zar_m,
       round(avg(m.won_value_zar) OVER (ORDER BY m.won_month
                 RANGE BETWEEN INTERVAL '2 months' PRECEDING
                           AND CURRENT ROW) / 1e6, 2)                     AS ma3_range_zar_m,
       count(*) OVER (ORDER BY m.won_month
                 ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)                AS rows_in_frame,
       count(*) OVER (ORDER BY m.won_month
                 RANGE BETWEEN INTERVAL '2 months' PRECEDING
                           AND CURRENT ROW)                               AS range_in_frame
FROM monthly m
ORDER BY m.won_month;


-- =====================================================================
-- Q05. BROKER LEADERBOARD FOR THE CURRENT YEAR
--
-- BUSINESS QUESTION: Who are the top performers this year, what share of the
--   book does each hold, and which quartile does every broker fall into?
-- TECHNIQUE: Four window functions over one shared WINDOW clause.
--
--   RANK VERSUS DENSE_RANK: both are here because they answer different
--   questions and the difference only shows up on ties. Ranking by an integer
--   deal count produces plenty of ties. RANK leaves gaps, so after two brokers
--   tie at 3 the next is 5, which is what you want when the question is "how
--   many brokers are ahead of me". DENSE_RANK does not skip, so the next is 4,
--   which is what you want when the question is "how many distinct performance
--   levels are above me". Publishing one and calling it the other is a
--   reporting bug that is almost impossible to spot downstream.
--
--   tied_peers uses RANGE BETWEEN CURRENT ROW AND CURRENT ROW, which in RANGE
--   mode means "every row with the same ORDER BY value", so it returns the size
--   of the tie group. That is the crispest available demonstration that RANGE is
--   about values and ROWS is about positions: the identical clause under ROWS
--   would always return 1.
--
--   NTILE(4) is computed over EVERY broker before the LIMIT applies, so the
--   quartiles describe the whole population and not just the visible rows.
--   Window functions run after WHERE and GROUP BY but before LIMIT, which is
--   exactly why this works.
--
--   THE TWO HOP JOIN, AND THE BUG IT FIXES. broker_sk on the fact is a VERSION
--   key, not a person key: the loader resolves it to whichever dim_broker row
--   was in force on the day of the deal. So "JOIN dim_broker ON broker_sk AND
--   is_current" is not a filter, it is an inner join that silently DELETES
--   every deal attributed to a superseded version. Measured on this warehouse,
--   49,049 of 248,940 pipeline rows carry a non current broker_sk, and that one
--   word took the leaderboard from 766 broker versions down to 736 and dropped
--   772 deals. Worse, pct_of_total_won_value is a sum() OVER () evaluated after
--   the join, so every published share was a share of an already truncated
--   book. The fix is the pattern Q14 uses: hop to the historic version by
--   surrogate key, then hop back out through the natural key to the current
--   row. That is what actually pins the leaderboard to today's org chart, which
--   is the right choice for "who do I congratulate", without throwing history
--   away to do it.
-- =====================================================================
WITH broker_book AS (
    -- Grain: one row per PERSON, keyed by the current version of the broker.
    -- Aggregating after the two hop resolution is deliberate: a broker who
    -- changed tier or agency mid year has two surrogate keys in the fact, and
    -- grouping on the raw surrogate would list them twice on a leaderboard with
    -- their year split in half. 24 of this year's 740 named brokers are in
    -- exactly that position. (741 including the UNKNOWN member, which the
    -- broker_sk > 0 filter below removes.)
    --
    -- Restricted to the latest reporting year present in the data rather than
    -- to a literal, so the leaderboard follows the warehouse instead of the
    -- calendar and does not silently go empty the first time it runs in a year
    -- the data has not reached.
    --
    -- This filter is served by ix_fct_pipeline_received as a range scan. There
    -- is no partition pruning here: mart.fct_deal_pipeline is deliberately NOT
    -- partitioned (see section 5 of 02_warehouse.sql), and calling an index
    -- range scan "pruning" confuses two different mechanisms.
    SELECT curr.broker_sk                                     AS broker_sk,
           count(*)                                          AS deals,
           count(*) FILTER (WHERE p.is_won)                   AS deals_won,
           COALESCE(sum(p.deal_value_zar)
                    FILTER (WHERE p.is_won), 0)               AS won_value_zar,
           COALESCE(sum(p.deal_value_zar)
                    FILTER (WHERE p.is_open), 0)              AS open_value_zar
    FROM mart.fct_deal_pipeline p
    -- Hop one: the version of the broker that was in force at deal time.
    JOIN mart.dim_broker hist ON hist.broker_sk = p.broker_sk
    -- Hop two: back out through the natural key to today's row.
    JOIN mart.dim_broker curr ON curr.broker_nk = hist.broker_nk
                             AND curr.is_current
    WHERE p.received_date >= date_trunc('year',
              (SELECT max(received_date) FROM mart.fct_deal_pipeline))
      -- The warehouse's UNKNOWN placeholder is is_current by design, so it
      -- would otherwise rank third on a list of human beings. A leaderboard
      -- names people. Same convention as Q13's agency_sk > 0.
      AND curr.broker_sk > 0
    GROUP BY curr.broker_sk
)
SELECT b.broker_nk,
       b.full_name,
       b.agency_name,
       b.broker_tier,
       bb.deals,
       bb.deals_won,
       round(bb.won_value_zar / 1e6, 2)                                  AS won_zar_m,
       round(bb.open_value_zar / 1e6, 2)                                 AS open_zar_m,
       rank()       OVER w                                               AS rank_by_wins,
       dense_rank() OVER w                                               AS dense_rank_by_wins,
       count(*) OVER (ORDER BY bb.deals_won DESC
                      RANGE BETWEEN CURRENT ROW AND CURRENT ROW)         AS tied_peers,
       round(100.0 * bb.won_value_zar
             / NULLIF(sum(bb.won_value_zar) OVER (), 0), 2)              AS pct_of_total_won_value,
       ntile(4) OVER w                                                   AS wins_quartile
FROM broker_book bb
-- bb.broker_sk is already the CURRENT version, resolved by the two hop join in
-- the CTE, so this is a plain lookup and cannot drop a row. Q14 shows the same
-- two hop pattern used for the opposite purpose: attribution as it was.
JOIN mart.dim_broker b ON b.broker_sk = bb.broker_sk
WINDOW w AS (ORDER BY bb.deals_won DESC)
ORDER BY rank_by_wins, bb.won_value_zar DESC
LIMIT 20;


-- =====================================================================
-- Q06. SECTOR BY INTAKE CHANNEL PIVOT
--
-- BUSINESS QUESTION: How does each property sector's deal book split across
--   our five intake channels, and which channels bring the bigger deals?
-- TECHNIQUE: Aggregate FILTER to pivot rows into columns in a single pass.
--
--   WHY FILTER BEATS CASE WHEN, three concrete reasons:
--
--   1. CASE WHEN inside COUNT is a trap. count(CASE WHEN c = 'Portal' THEN 1 END)
--      is correct only because COUNT ignores NULLs. Add the ELSE 0 that looks
--      tidier and it silently counts every row in the group. FILTER cannot be
--      got wrong that way because the predicate is separated from the value.
--
--   2. FILTER applies to ANY aggregate, including COUNT(DISTINCT ...) and
--      ordered set aggregates. count(DISTINCT CASE WHEN ... END) quietly counts
--      NULL as nothing and works by accident, and there is no CASE form at all
--      for percentile_cont ... WITHIN GROUP. Both are used below.
--
--   3. It reads as the business rule. "count the deals, where the channel is
--      Portal" is the sentence a stakeholder said out loud.
--
--   Performance is identical: both are one pass over the group. This is a
--   correctness and readability argument, not a speed one, and claiming
--   otherwise would be easy to disprove.
-- =====================================================================
SELECT pr.sector,
       count(*)                                                            AS deals,
       count(*) FILTER (WHERE ds.source_channel = 'Email intake')           AS ch_email,
       count(*) FILTER (WHERE ds.source_channel = 'Portal')                 AS ch_portal,
       count(*) FILTER (WHERE ds.source_channel = 'Referral')               AS ch_referral,
       count(*) FILTER (WHERE ds.source_channel = 'Direct call')            AS ch_direct,
       count(*) FILTER (WHERE ds.source_channel = 'Auction notice')         AS ch_auction,
       round(100.0 * count(*) FILTER (WHERE ds.is_off_market) / count(*), 1) AS off_market_pct,
       -- FILTER on COUNT(DISTINCT ...): how many separate brokers actually
       -- converted something in this sector, not how many deals converted.
       count(DISTINCT p.broker_sk) FILTER (WHERE p.is_won)                  AS brokers_with_a_win,
       -- FILTER on an ordered set aggregate. There is no CASE WHEN equivalent.
       -- The ::numeric cast is required, not cosmetic: percentile_cont has no
       -- numeric overload, so a numeric input is promoted to double precision and
       -- round(double precision, integer) does not exist in PostgreSQL.
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY p.deal_value_zar)
              FILTER (WHERE ds.source_channel = 'Referral'))::numeric / 1e6, 2)       AS median_referral_zar_m,
       round((percentile_cont(0.5) WITHIN GROUP (ORDER BY p.deal_value_zar)
              FILTER (WHERE ds.source_channel = 'Auction notice'))::numeric / 1e6, 2) AS median_auction_zar_m
FROM mart.fct_deal_pipeline p
JOIN mart.dim_property pr    ON pr.property_sk = p.property_sk
JOIN mart.dim_deal_source ds ON ds.deal_source_sk = p.deal_source_sk
GROUP BY pr.sector
ORDER BY deals DESC;


-- =====================================================================
-- Q07. DEAL VALUE DISTRIBUTION AND HISTOGRAM
--
-- BUSINESS QUESTION: What does our deal size distribution actually look like,
--   and is the average deal value a number anyone should be quoting?
-- TECHNIQUE: WIDTH_BUCKET for fixed width bucketing, plus PERCENTILE_CONT and
--   STDDEV_SAMP for the shape.
--
--   WHY BUCKET log10 AND NOT RAND: deal value is log-normal, spanning a
--   measured R328.82 to R4,116,545,082.02, which is seven orders of magnitude.
--   WIDTH_BUCKET on raw Rand would put well over 90% of deals in bucket one and
--   show a single spike. Bucketing log10 of the value gives evenly spaced
--   half decades and the underlying bell shape becomes visible. Choosing the
--   scale to match the distribution is the whole job here.
--
--   WHAT WIDTH_BUCKET DOES OUTSIDE ITS BOUNDS, and why that matters. Given
--   width_bucket(x, lo, hi, n) it returns 0 for x below lo and n+1 for x at or
--   above hi. Those two buckets are UNBOUNDED on one side, so any band label
--   computed arithmetically from the bucket number is a fabrication for them:
--   bucket 0 would print a lower edge that most of its own contents sit below.
--   An earlier version of this query used bounds 4.5 to 9.5 and asserted that
--   nothing fell outside them. That was false, 603 deals sat in bucket 0 and 3
--   in bucket 11, and both printed invented bands. Two things fix it and both
--   are here: the bounds now sit outside the measured range, and the band
--   labels are still guarded so that if a future load moves the range the
--   out of range edge comes back NULL rather than made up.
--
--   WHY STDDEV_SAMP AND NOT STDDEV_POP: these deals are a sample of the market,
--   not the entire population of it, so the sample estimator with its n minus 1
--   denominator is the correct one. At this row count the two agree to several
--   decimal places, but picking the right one on purpose is the point.
--
--   The mean sitting far above the median in every bucket is not a data defect,
--   it is what right skew looks like, and it is the direct evidence that
--   average deal value is the wrong headline number.
-- =====================================================================
WITH valued AS (
    SELECT p.deal_value_zar,
           log(10, p.deal_value_zar) AS log_value
    FROM mart.fct_deal_pipeline p
    WHERE p.deal_value_zar > 0
),
bucketed AS (
    -- Bounds 2.5 to 10.0 in log10 terms, fifteen buckets of half a decade each.
    -- The measured range is log10 2.5169582 (R328.82) to log10 9.6145329
    -- (R4,116,545,082.02), so both bounds sit outside the data and every deal
    -- lands in buckets 1 through 15. Verified: buckets 0 and 16 are empty.
    SELECT v.deal_value_zar,
           v.log_value,
           width_bucket(v.log_value, 2.5, 10.0, 15) AS log_bucket
    FROM valued v
)
SELECT b.log_bucket,
       -- The CASE guards are not decoration. Bucket 0 has no lower edge and
       -- bucket 16 has no upper edge, so the arithmetic label is only valid for
       -- 1 through 15. NULL is the honest answer for an unbounded side.
       CASE WHEN b.log_bucket = 0 THEN NULL
            ELSE round(power(10, 2.5 + (b.log_bucket - 1) * 0.5)::numeric / 1e6, 3)
       END                                                                 AS band_from_zar_m,
       CASE WHEN b.log_bucket = 16 THEN NULL
            ELSE round(power(10, 2.5 + b.log_bucket * 0.5)::numeric / 1e6, 3)
       END                                                                 AS band_to_zar_m,
       count(*)                                                           AS deals,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2)                  AS pct_of_deals,
       -- Scaled to the tallest bucket so the shape is readable in a terminal.
       repeat('#', (50.0 * count(*) / max(count(*)) OVER ())::integer)      AS shape,
       -- ::numeric is required on every percentile_cont result: the function has
       -- no numeric overload, so numeric input is promoted to double precision and
       -- round(double precision, integer) does not exist in PostgreSQL.
       round((percentile_cont(0.5) WITHIN GROUP
              (ORDER BY b.deal_value_zar))::numeric / 1e6, 2)                AS median_zar_m,
       round((percentile_cont(0.9) WITHIN GROUP
              (ORDER BY b.deal_value_zar))::numeric / 1e6, 2)                AS p90_zar_m,
       round(avg(b.deal_value_zar) / 1e6, 2)                                AS mean_zar_m,
       round(stddev_samp(b.deal_value_zar) / 1e6, 2)                        AS stddev_zar_m,
       -- Above 1.0 means the mean overstates the typical deal in this band.
       round(avg(b.deal_value_zar)
             / (percentile_cont(0.5) WITHIN GROUP (ORDER BY b.deal_value_zar))::numeric, 3)
                                                                            AS mean_over_median
FROM bucketed b
GROUP BY b.log_bucket
ORDER BY b.log_bucket;


-- =====================================================================
-- Q08. OPEN DEALS AT RISK OF STALLING
--
-- BUSINESS QUESTION: Which deals are still open but have gone quiet for longer
--   than their stage allows, and how much value is exposed?
-- TECHNIQUE: A per deal last touch via DISTINCT ON, a date difference against a
--   pinned as at date, and a threshold read from the dimension.
--
--   THE THRESHOLD IS DATA, NOT CODE. dim_stage.target_days_in_stage carries each
--   stage's SLA, and the rule is "silent for more than twice its own SLA".
--   Changing the business definition of at risk is then an UPDATE to eleven rows
--   in a dimension, not an edit to a query that has been copied into six
--   dashboards. Hardcoding 90 days here would also be wrong on its face:
--   Received allows one day and Legal allows thirty, so one global number
--   cannot be right for both.
--
--   WHY as_at IS PINNED TO THE DATA. Using CURRENT_DATE would make the answer
--   drift every night and make the committed output impossible to reproduce.
--   The as at date is the newest event in the warehouse, which is also the
--   honest statement of how current the answer can possibly be.
--
--   AND WHY IT IS ALSO BOUNDED. Pinning to max(event_date) with nothing else is
--   not enough, and this query proved it. The generator injects 369 deliberately
--   out of range events dated 2031, which ASSERT-STG-018 and DQ-012 both flag
--   and which 04_facts.sql parks in the DEFAULT partition. max(event_date) over
--   the whole fact therefore returned 2031-12-30, five years past the real
--   warehouse horizon, so every "days silent" figure was inflated by about
--   1,978 days. The minimum silence across the entire open book came out at 143
--   days, which is more than twice the largest SLA, so EVERY stage reported
--   exactly 100 percent at risk and the report told a manager nothing. Bounding
--   as_at to the same horizon the partition range enforces (04_facts.sql) fixes
--   it: the stages now separate cleanly, from 24.5 percent of the Due diligence
--   book at risk to 80.2 percent of Triaged. A single unbounded max() is how a
--   correctly flagged data quality problem walks into a business answer anyway.
--
--   ONE GRAIN FOR THE RATIO. pct_of_stage_open_book used to divide a numerator
--   counted from the EVENT fact by a denominator counted from the PIPELINE
--   fact's current_stage_sk column. Those are two different grains and they
--   disagree: 238 open deals exist in fct_deal_pipeline with no rows at all in
--   fct_deal_stage_event, which is why Received alone came out at 85.4 percent
--   while every other stage sat at exactly 100.0. Both sides are now counted
--   from last_touch, so the ratio is a share of the same population it is drawn
--   from. This is the file header's warning about mixing grains, caught in the
--   file's own code.
--
--   DISTINCT ON is Postgres specific and does in one pass what a portable query
--   needs ROW_NUMBER plus an outer filter to do. Q09 shows the portable form,
--   which is the one to reach for when the SQL has to run elsewhere too.
-- =====================================================================
WITH as_at AS (
    -- DATE '2027-01-01' is the warehouse horizon, the same bound that closes
    -- the last named partition in 04_facts.sql. Anything at or beyond it is
    -- known bad data sitting in the DEFAULT partition on purpose.
    SELECT max(f.event_date) AS as_at_date
    FROM mart.fct_deal_stage_event f
    WHERE f.event_date < DATE '2027-01-01'
),
last_touch AS (
    SELECT DISTINCT ON (f.deal_nk)
           f.deal_nk,
           f.event_date  AS last_event_date,
           f.to_stage_sk AS current_stage_sk,
           f.broker_sk,
           f.deal_value_zar
    FROM mart.fct_deal_stage_event f
    WHERE f.event_date < DATE '2027-01-01'
    ORDER BY f.deal_nk, f.event_ts DESC, f.deal_event_sk DESC
),
open_touch AS (
    -- The population this query is about: open deals, each reduced to its most
    -- recent event. Every number below, numerator and denominator, comes from
    -- here so the ratio cannot mix grains.
    SELECT lt.*
    FROM last_touch lt
    JOIN mart.fct_deal_pipeline p ON p.deal_nk = lt.deal_nk AND p.is_open
),
open_by_stage AS (
    SELECT ot.current_stage_sk,
           count(*) AS open_in_stage
    FROM open_touch ot
    GROUP BY ot.current_stage_sk
)
SELECT s.stage_name,
       s.target_days_in_stage                                     AS sla_days,
       s.target_days_in_stage * 2                                 AS at_risk_after_days,
       count(*)                                                   AS open_deals_at_risk,
       obs.open_in_stage                                          AS open_deals_in_stage,
       round(sum(ot.deal_value_zar) / 1e9, 2)                     AS value_at_risk_zar_bn,
       round(avg(a.as_at_date - ot.last_event_date), 1)           AS avg_days_silent,
       max(a.as_at_date - ot.last_event_date)                     AS max_days_silent,
       count(DISTINCT ot.broker_sk)                               AS brokers_involved,
       -- The share of this stage's open book that is at risk, which is the
       -- number that tells a manager where to intervene. Numerator and
       -- denominator are both counted from open_touch.
       round(100.0 * count(*) / obs.open_in_stage, 1)             AS pct_of_stage_open_book
FROM open_touch ot
JOIN mart.dim_stage s     ON s.stage_sk = ot.current_stage_sk
JOIN open_by_stage obs    ON obs.current_stage_sk = ot.current_stage_sk
CROSS JOIN as_at a
WHERE s.target_days_in_stage IS NOT NULL
  AND (a.as_at_date - ot.last_event_date) > s.target_days_in_stage * 2
GROUP BY s.stage_name, s.stage_order, s.target_days_in_stage, obs.open_in_stage
ORDER BY value_at_risk_zar_bn DESC;


-- =====================================================================
-- Q09. ONE DEFINITIVE ROW PER DEAL PER STAGE
--
-- BUSINESS QUESTION: The stage entry report must show each deal once per stage.
--   Deals get reopened and re-enter stages they have already been through, so
--   which entry is the one of record?
-- TECHNIQUE: ROW_NUMBER inside a CTE, filtered OUTSIDE it. This is the portable
--   equivalent of Snowflake and BigQuery's QUALIFY clause, which Postgres does
--   not have.
--
--   WHY THE FILTER CANNOT GO INSIDE. Window functions are evaluated AFTER WHERE,
--   so "WHERE row_number() OVER (...) = 1" is not just disallowed, it is
--   logically impossible: at the time WHERE runs, the window has not been
--   computed. The value must be projected in one query level and filtered in
--   the next. In Snowflake QUALIFY rn = 1 is sugar for exactly this extra level.
--
--   WHY THE BEST ROW IS THE LATEST. The business rule is that a reopened deal's
--   most recent entry into a stage supersedes the earlier one. deal_event_sk
--   breaks any tie on the timestamp so the result is deterministic; without a
--   unique tiebreaker the same query can return different rows on different
--   runs, which is a genuinely nasty class of bug because it passes every count
--   based test. This is real work here, not a demonstration: 95,550 reopened
--   entries are suppressed, 8.2 percent of the 1,165,043 event rows. The
--   suppressed_reentries column below sums to exactly that figure, so the claim
--   is checkable against the output printed underneath it.
--
--   NOTHING IS DELETED. The suppressed rows stay in the fact and the count of
--   them is reported, so the reopen rate remains an answerable question.
-- =====================================================================
WITH ranked_entry AS (
    SELECT f.deal_nk,
           f.to_stage_sk,
           f.event_ts,
           f.event_date,
           f.broker_sk,
           f.deal_value_zar,
           f.days_in_prev_stage,
           row_number() OVER (PARTITION BY f.deal_nk, f.to_stage_sk
                              ORDER BY f.event_ts DESC, f.deal_event_sk DESC) AS rn,
           count(*)     OVER (PARTITION BY f.deal_nk, f.to_stage_sk)          AS entries_for_key
    FROM mart.fct_deal_stage_event f
),
deduped AS (
    -- The QUALIFY equivalent: filter on the window result one level out.
    SELECT * FROM ranked_entry WHERE rn = 1
)
SELECT s.stage_name,
       count(*)                                                        AS definitive_rows,
       sum(d.entries_for_key) - count(*)                               AS suppressed_reentries,
       count(*) FILTER (WHERE d.entries_for_key > 1)                    AS deals_reopened_here,
       round(100.0 * count(*) FILTER (WHERE d.entries_for_key > 1)
             / count(*), 2)                                            AS reopen_rate_pct,
       round(avg(d.days_in_prev_stage), 2)                             AS avg_days_to_reach,
       round(sum(d.deal_value_zar) / 1e9, 2)                           AS value_entering_zar_bn
FROM deduped d
JOIN mart.dim_stage s ON s.stage_sk = d.to_stage_sk
GROUP BY s.stage_name, s.stage_order
ORDER BY s.stage_order;


-- =====================================================================
-- Q10. SECTOR CONCENTRATION RISK IN THE OPEN PIPELINE
--
-- BUSINESS QUESTION: How much of each sector's open pipeline depends on a
--   handful of brokers? If our top people walked out, what walks out with them?
-- TECHNIQUE: A running share within each sector using SUM over an ordered
--   window frame, then read off at specific ranks with aggregate FILTER.
--
--   WHY A CUMULATIVE WINDOW AND NOT A TOP N SUBQUERY: one pass gives the share
--   at every rank at once, so top 3, top 5, top 10 and "how many brokers make
--   up half the book" all come out of the same scan. The alternative is four
--   correlated subqueries that each re-read the sector.
--
--   brokers_to_half_the_book is the metric to actually watch. A percentage at a
--   fixed rank hides the population size; "seventeen brokers hold half this
--   sector" is a sentence an executive can act on immediately.
--
--   HHI is the Herfindahl Hirschman index, the sum of squared percentage
--   shares, and it is here because it is the standard concentration measure
--   rather than a number invented for this report. Above 1,500 is conventionally
--   treated as moderately concentrated and above 2,500 as highly concentrated,
--   so the figure can be compared against something external.
--
--   NULLS LAST IS LOAD BEARING, and this query shipped without it. DESC in
--   PostgreSQL defaults to NULLS FIRST, which is the same trap Q04 documents.
--   1.168 percent of deals (2,907 of 248,940) carry a NULL deal_value_zar, and
--   they produced 16 (sector, broker) groups whose open_value_zar summed to
--   NULL. Those groups sorted to broker_rank 1, so the ranks that this report
--   reads off were pointing at placeholders. Measured by re-running the broken
--   form against this warehouse: largest_single_broker_pct came back NULL for
--   SEVEN of nine sectors and top3_share_pct for three. top5_share_pct was
--   worse than NULL, because by rank 5 at least one real broker had entered the
--   cumulative sum: it came back silently UNDERSTATED for every sector, since
--   ranks 1 through 4 were placeholders contributing nothing. Retail published
--   5.18 percent against a true 14.18, Industrial 10.96 against 16.95,
--   Healthcare 13.09 against 18.26. A concentration report that understates the
--   top 5 share by nine points is worse than no report, because it reads as a
--   plausible number rather than as an error. Two lines fix it and both are
--   below: NULLS LAST on the window ORDER BY, and COALESCE on the aggregate so
--   a broker with no priced deals ranks at zero instead of at NULL. Same policy
--   as Q04, one lesson.
-- =====================================================================
WITH open_book AS (
    -- Grain: one row per sector per broker. Open deals only, since a closed
    -- deal carries no forward risk. COALESCE to 0 so that a broker whose open
    -- deals are all unpriced carries a real zero rather than a NULL that can
    -- poison a cumulative window downstream.
    SELECT pr.sector,
           p.broker_sk,
           COALESCE(sum(p.deal_value_zar), 0) AS open_value_zar,
           count(*)                           AS open_deals
    FROM mart.fct_deal_pipeline p
    JOIN mart.dim_property pr ON pr.property_sk = p.property_sk
    WHERE p.is_open
    GROUP BY pr.sector, p.broker_sk
),
ranked AS (
    SELECT o.sector,
           o.broker_sk,
           o.open_value_zar,
           o.open_deals,
           row_number() OVER w                                    AS broker_rank,
           count(*)  OVER (PARTITION BY o.sector)                  AS brokers_active,
           sum(o.open_value_zar) OVER (PARTITION BY o.sector)      AS sector_value,
           sum(o.open_value_zar) OVER (w ROWS BETWEEN UNBOUNDED PRECEDING
                                                  AND CURRENT ROW) AS cum_value
    FROM open_book o
    WINDOW w AS (PARTITION BY o.sector ORDER BY o.open_value_zar DESC NULLS LAST,
                                                o.broker_sk)
),
shares AS (
    SELECT r.*,
           100.0 * r.cum_value       / r.sector_value AS cum_share_pct,
           100.0 * r.open_value_zar  / r.sector_value AS own_share_pct
    FROM ranked r
)
SELECT s.sector,
       max(s.brokers_active)                                             AS brokers_active,
       round(max(s.sector_value) / 1e9, 2)                               AS open_value_zar_bn,
       round(max(s.cum_share_pct) FILTER (WHERE s.broker_rank = 3),  2)  AS top3_share_pct,
       round(max(s.cum_share_pct) FILTER (WHERE s.broker_rank = 5),  2)  AS top5_share_pct,
       round(max(s.cum_share_pct) FILTER (WHERE s.broker_rank = 10), 2)  AS top10_share_pct,
       round(max(s.own_share_pct) FILTER (WHERE s.broker_rank = 1),  2)  AS largest_single_broker_pct,
       count(*) FILTER (WHERE s.cum_share_pct <= 50)                     AS brokers_to_half_the_book,
       round(sum(power(s.own_share_pct, 2)))                             AS hhi,
       CASE WHEN sum(power(s.own_share_pct, 2)) > 2500 THEN 'highly concentrated'
            WHEN sum(power(s.own_share_pct, 2)) > 1500 THEN 'moderately concentrated'
            ELSE 'competitive'
       END                                                               AS hhi_verdict
FROM shares s
GROUP BY s.sector
ORDER BY top5_share_pct DESC;


-- =====================================================================
-- Q11. BROKER COHORT RETENTION
--
-- BUSINESS QUESTION: Of the brokers who submitted their first deal in a given
--   quarter, what share are still submitting one, two, four, eight quarters
--   later? Are recent intakes sticking better or worse than older ones?
-- TECHNIQUE: Cohort assignment from the first observed deal, a FULL COHORT GRID
--   built by CROSS JOIN against generate_series, then a pivot into the classic
--   retention triangle.
--
--   WHY THE GRID EXISTS. Without it, a cohort that went completely silent in
--   quarter five simply has no row for quarter five, and the reader's eye joins
--   quarter four straight to quarter six and concludes the cohort recovered.
--   The grid manufactures every (cohort, offset) cell that COULD be observed,
--   then LEFT JOINs the activity onto it, so silence arrives as an explicit 0.0.
--
--   WHAT THE GRID ACTUALLY DID ON THIS DATA: nothing. All 377 cells have a
--   matching activity row, the COALESCE never fires, and silent_quarters is 0
--   for all 26 cohorts. Saying otherwise would be claiming a save that did not
--   happen. The grid stays anyway, and that is a design argument rather than a
--   measurement: the guarantee that silence is visible must not depend on
--   whether this particular load happens to contain any. One silent quarter in
--   a future load would otherwise vanish from the triangle, and nobody would
--   know to look for it.
--
--   ZERO AND NULL MEAN DIFFERENT THINGS HERE, and conflating them is the
--   commonest cohort reporting error:
--     0.0  the cohort existed, the quarter has happened, nobody submitted.
--     NULL that quarter has not happened yet for this cohort. Not measurable.
--   The grid's WHERE clause is what draws that line, which is why the output is
--   a triangle rather than a rectangle.
--
--   q0 is 100.0 by construction, since a cohort is DEFINED by its first deal.
--   That is a useful sanity check, not a finding: if any q0 came back below
--   100.0 the cohort assignment would be broken.
--
--   THE FIRST COHORT IS LEFT CENSORED, and the triangle cannot be read without
--   knowing it. A cohort here is the quarter of a broker's first deal INSIDE
--   THE WAREHOUSE WINDOW, and the window opens on 2020-01-01. Every broker who
--   was already working before that date therefore lands in 2020-Q1 regardless
--   of when they actually started, which is 753 of 1,132 brokers, 66.5 percent
--   of the population. 2020-Q1 is the only cohort large enough to read: 25 of
--   the 26 cohorts hold fewer than 30 brokers and the smallest holds 8, where a
--   single broker moves the retention figure by 12.5 points. So the decay down
--   the 2020-Q1 row (q0 100.0, q1 97.7, q2 95.5, q3 90.8, q4 90.8, q6 87.9,
--   q8 83.3, q12 76.9, q16 70.7) is the signal, and the small cohorts below it
--   are noise that happens to be
--   arranged in a triangle. Fixing this properly needs a broker start date from
--   the source system, which this warehouse does not receive.
-- =====================================================================
WITH broker_first AS (
    -- Cohort = the quarter of the broker's first ever submission.
    SELECT p.broker_sk,
           date_trunc('quarter', min(p.received_date))::date AS cohort_quarter
    FROM mart.fct_deal_pipeline p
    GROUP BY p.broker_sk
),
cohort_size AS (
    SELECT cohort_quarter, count(*) AS cohort_brokers
    FROM broker_first
    GROUP BY cohort_quarter
),
activity AS (
    -- One row per broker per quarter in which they submitted anything at all.
    SELECT DISTINCT p.broker_sk,
           date_trunc('quarter', p.received_date)::date AS active_quarter
    FROM mart.fct_deal_pipeline p
),
bounds AS (
    SELECT date_trunc('quarter', max(received_date))::date AS last_quarter
    FROM mart.fct_deal_pipeline
),
grid AS (
    -- Every cell that could be observed. Offsets beyond the end of the data are
    -- excluded here so they surface as NULL rather than as a misleading 0.
    SELECT cs.cohort_quarter,
           cs.cohort_brokers,
           g.quarters_since
    FROM cohort_size cs
    CROSS JOIN generate_series(0, 26) AS g(quarters_since)
    CROSS JOIN bounds b
    WHERE (cs.cohort_quarter + (g.quarters_since * interval '3 months'))::date
          <= b.last_quarter
),
retained AS (
    -- Quarter offset computed as a whole number of quarters rather than by
    -- differencing dates, so a 92 day quarter and an 89 day quarter both land on
    -- the same offset.
    SELECT bf.cohort_quarter,
           ((EXTRACT(YEAR    FROM a.active_quarter) - EXTRACT(YEAR    FROM bf.cohort_quarter)) * 4
          + (EXTRACT(QUARTER FROM a.active_quarter) - EXTRACT(QUARTER FROM bf.cohort_quarter)))::integer
               AS quarters_since,
           count(DISTINCT a.broker_sk) AS active_brokers
    FROM activity a
    JOIN broker_first bf ON bf.broker_sk = a.broker_sk
    GROUP BY 1, 2
),
cells AS (
    SELECT g.cohort_quarter,
           g.cohort_brokers,
           g.quarters_since,
           COALESCE(r.active_brokers, 0) AS active_brokers,
           round(100.0 * COALESCE(r.active_brokers, 0) / g.cohort_brokers, 1) AS retention_pct
    FROM grid g
    LEFT JOIN retained r
           ON r.cohort_quarter = g.cohort_quarter
          AND r.quarters_since = g.quarters_since
)
SELECT c.cohort_quarter,
       max(c.cohort_brokers)                                    AS cohort_brokers,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 0)  AS q0,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 1)  AS q1,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 2)  AS q2,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 3)  AS q3,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 4)  AS q4,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 6)  AS q6,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 8)  AS q8,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 12) AS q12,
       max(c.retention_pct) FILTER (WHERE c.quarters_since = 16) AS q16,
       count(*) FILTER (WHERE c.active_brokers = 0)              AS silent_quarters
FROM cells c
GROUP BY c.cohort_quarter
ORDER BY c.cohort_quarter;


-- =====================================================================
-- Q12. GAPS AND ISLANDS: UNBROKEN RUNS OF BROKER ACTIVITY
--
-- BUSINESS QUESTION: Which brokers have submitted every single month without a
--   break, and who has just broken a long run and needs a call this week?
-- TECHNIQUE: The classic gaps and islands solution by difference of row number.
--
--   HOW THE TRICK WORKS, in one sentence: for a run of CONSECUTIVE months the
--   month advances by exactly one and the row number advances by exactly one, so
--   subtracting the row number from the month gives the SAME constant for every
--   month in the run, and that constant is a ready made group key. The moment a
--   month is missing the month jumps by two while the row number jumps by one,
--   the constant shifts, and a new island begins.
--
--   WHY NOT SELF JOIN each row to the row before it and hunt for breaks: that
--   finds the boundaries but then needs a second pass to stitch each boundary
--   back into a run. The difference trick labels every row with its run in a
--   single window pass, and it is O(n log n) on the sort rather than O(n) joins.
--
--   THE ROW NUMBER MUST BE DENSE AND GAP FREE, which is why ROW_NUMBER is
--   correct and RANK is not: RANK leaves gaps on ties and the arithmetic breaks
--   instantly. The DISTINCT in broker_month guarantees one row per month per
--   broker so no tie can arise in the first place.
--
--   THE GROUPING KEY MUST BE THE PERSON, NOT THE VERSION. This query originally
--   grouped by the raw broker_sk from the fact, which is a Type 2 VERSION key.
--   A broker who changed agency in 2023 has two surrogate keys, so an unbroken
--   run of activity from 2020 to 2026 was cut into two streaks by a change in a
--   dimension attribute, not by any gap in the broker's actual behaviour. That
--   is precisely the wrong answer to "who has submitted every single month
--   without a break". Measured over the same population of people, so the two
--   sides are comparable: grouping by VERSION spreads those people across 1,131
--   surrogate keys and produces 8,035 islands, grouping by PERSON gives 800
--   brokers and 7,801 islands. So 234 island boundaries were dimension attribute
--   changes rather than real gaps in anybody's behaviour. (Over the whole fact,
--   including the UNKNOWN member, version grouping gives 8,036 islands across
--   1,132 surrogate keys.)
--
--   The display join had the same defect in a more visible form:
--   "JOIN dim_broker ON broker_sk AND is_current" is an inner join that dropped
--   every streak belonging to a superseded version outright. Both are fixed by
--   resolving to the natural key first, the same two hop that Q05 and Q14 use.
--
--   This is real on this data, not a set piece: 800 named brokers resolve into
--   7,801 islands, so the average broker's history is broken into just under
--   ten separate runs.
-- =====================================================================
WITH broker_month AS (
    -- DISTINCT collapses many deals in a month to a single activity marker,
    -- which is what makes ROW_NUMBER dense and the arithmetic valid.
    --
    -- The two hop resolution happens HERE, before the islands are built, so a
    -- version change cannot masquerade as a gap. curr.broker_sk > 0 drops the
    -- UNKNOWN placeholder, because the closing line of this report is "who
    -- needs a call this week" and nobody can call Unknown broker.
    SELECT DISTINCT curr.broker_sk AS broker_sk,
           date_trunc('month', p.received_date)::date AS active_month
    FROM mart.fct_deal_pipeline p
    JOIN mart.dim_broker hist ON hist.broker_sk = p.broker_sk
    JOIN mart.dim_broker curr ON curr.broker_nk = hist.broker_nk
                             AND curr.is_current
    WHERE curr.broker_sk > 0
),
sequenced AS (
    SELECT bm.broker_sk,
           bm.active_month,
           row_number() OVER (PARTITION BY bm.broker_sk
                              ORDER BY bm.active_month) AS rn
    FROM broker_month bm
),
islands AS (
    SELECT s.broker_sk,
           s.active_month,
           -- Constant within a run of consecutive months, so it is the island key.
           (s.active_month - (s.rn * interval '1 month'))::date AS island_key
    FROM sequenced s
),
streaks AS (
    SELECT i.broker_sk,
           i.island_key,
           min(i.active_month) AS streak_start,
           max(i.active_month) AS streak_end,
           count(*)            AS streak_months
    FROM islands i
    GROUP BY i.broker_sk, i.island_key
),
last_month AS (
    SELECT date_trunc('month', max(received_date))::date AS latest_month
    FROM mart.fct_deal_pipeline
)
SELECT b.broker_nk,
       b.full_name,
       b.agency_name,
       b.broker_tier,
       st.streak_start,
       st.streak_end,
       st.streak_months,
       count(*) OVER (PARTITION BY st.broker_sk)                   AS total_streaks,
       -- A run that reaches the last month in the warehouse is still going. Any
       -- other run has ended, and the longest ended run is the retention story.
       (st.streak_end = lm.latest_month)                           AS streak_is_live,
       CASE WHEN st.streak_end < lm.latest_month
            THEN (EXTRACT(YEAR  FROM lm.latest_month) - EXTRACT(YEAR  FROM st.streak_end)) * 12
               + (EXTRACT(MONTH FROM lm.latest_month) - EXTRACT(MONTH FROM st.streak_end))
       END::integer                                                AS months_since_streak_ended,
       b.is_active                                                 AS broker_still_on_the_books
FROM streaks st
-- st.broker_sk is already the CURRENT version, resolved in broker_month, so
-- this is a plain lookup and cannot drop a streak.
JOIN mart.dim_broker b ON b.broker_sk = st.broker_sk
CROSS JOIN last_month lm
ORDER BY st.streak_months DESC, b.broker_nk
LIMIT 20;


-- =====================================================================
-- Q13. AGENCY GROUP STRUCTURE ROLLUP
--
-- BUSINESS QUESTION: Our agencies are a three level structure: national groups
--   own regional masters, which own local offices. What does each entire group
--   contribute once every subsidiary beneath it is counted, and which
--   subsidiaries carry their group?
-- TECHNIQUE: A recursive CTE walking dim_agency.parent_agency_sk from the roots
--   downward, carrying the root identity down the tree so a rollup becomes a
--   plain PARTITION BY.
--
--   WHY RECURSION IS GENUINELY NEEDED. The depth is a property of the DATA, not
--   of the query. A three level join chain would work today and silently drop a
--   whole branch the day someone inserts a fourth level, and it cannot answer
--   "everything beneath node X" at all. The recursive term does not care how
--   deep the tree goes.
--
--   THE ROOT IS CARRIED DOWN, not looked up afterwards. Because every row knows
--   its root_agency_sk, the group rollup is one window function instead of a
--   second recursive pass. That single decision is what keeps this query short.
--
--   THE CYCLE CLAUSE IS NOT DECORATION. parent_agency_sk is a self reference
--   with no constraint that can prevent A parenting B while B parents A, and a
--   cycle in a recursive CTE does not return a wrong answer, it runs forever.
--   Postgres 14 and later can detect this natively: CYCLE marks the row where a
--   path repeats and stops descending. Shipping a recursive query over
--   user maintained hierarchy data without a cycle guard is how a scheduled
--   report takes a server down at 3am.
--
--   WHY THERE IS NO LIMIT. There was one, LIMIT 25 with ORDER BY root_agency_name,
--   and it answered the stated question for 2 of the 20 groups: the output
--   stopped part way into the third alphabetically named group and 18 groups
--   were invisible. A rollup query whose whole point is "what does each entire
--   group contribute" cannot be truncated alphabetically. The tree is 60 nodes
--   across 20 roots, which is a whole screen, not a data dump, so every node
--   prints and the ORDER BY leads with the group total so the biggest group is
--   at the top.
-- =====================================================================
WITH RECURSIVE agency_tree AS (
    -- Anchor: the national groups. No parent, and the unknown member at -1 is
    -- excluded because it is a warehouse artefact, not a real company.
    SELECT a.agency_sk,
           a.agency_name,
           a.parent_agency_sk,
           a.agency_tier,
           a.agency_sk        AS root_agency_sk,
           a.agency_name      AS root_agency_name,
           1                  AS depth,
           a.agency_name::text AS org_path
    FROM mart.dim_agency a
    WHERE a.parent_agency_sk IS NULL
      AND a.agency_sk > 0
    UNION ALL
    -- Recursive term: attach each child to its parent and carry the root and the
    -- readable path down with it.
    SELECT c.agency_sk,
           c.agency_name,
           c.parent_agency_sk,
           c.agency_tier,
           t.root_agency_sk,
           t.root_agency_name,
           t.depth + 1,
           t.org_path || ' > ' || c.agency_name
    FROM mart.dim_agency c
    JOIN agency_tree t ON t.agency_sk = c.parent_agency_sk
)
CYCLE agency_sk SET is_cycle USING cycle_path,
agency_book AS (
    -- Deals are credited to the agency the broker belonged to AT THE TIME of the
    -- deal, which is why this joins dim_broker by surrogate key and does not
    -- filter is_current. A broker who moved in 2023 leaves their 2021 wins with
    -- the agency that actually earned them.
    SELECT b.agency_sk,
           count(*)                                                 AS deals,
           count(*) FILTER (WHERE p.is_won)                          AS deals_won,
           COALESCE(sum(p.deal_value_zar) FILTER (WHERE p.is_won), 0) AS won_value_zar,
           count(DISTINCT p.broker_sk)                               AS brokers
    FROM mart.fct_deal_pipeline p
    JOIN mart.dim_broker b ON b.broker_sk = p.broker_sk
    GROUP BY b.agency_sk
)
SELECT t.root_agency_name,
       t.depth,
       t.agency_tier,
       t.org_path,
       COALESCE(ab.deals, 0)                                              AS own_deals,
       COALESCE(ab.deals_won, 0)                                          AS own_deals_won,
       round(COALESCE(ab.won_value_zar, 0) / 1e9, 3)                      AS own_won_zar_bn,
       -- The rollup. Because the root came down the recursion, the whole subtree
       -- total is a window over the root, not another recursive pass.
       count(*) OVER (PARTITION BY t.root_agency_sk)                      AS nodes_in_group,
       sum(COALESCE(ab.deals, 0)) OVER (PARTITION BY t.root_agency_sk)    AS group_deals,
       round(sum(COALESCE(ab.won_value_zar, 0))
             OVER (PARTITION BY t.root_agency_sk) / 1e9, 3)               AS group_won_zar_bn,
       round(100.0 * COALESCE(ab.won_value_zar, 0)
             / NULLIF(sum(COALESCE(ab.won_value_zar, 0))
                      OVER (PARTITION BY t.root_agency_sk), 0), 2)        AS pct_of_group_won,
       t.is_cycle                                                        AS cycle_detected
FROM agency_tree t
LEFT JOIN agency_book ab ON ab.agency_sk = t.agency_sk
-- Groups are ranked by what the whole group won, then the tree is walked from
-- the root down inside each group. Ordering by the group total first is what
-- makes the top of the output the answer to the business question.
ORDER BY sum(COALESCE(ab.won_value_zar, 0)) OVER (PARTITION BY t.root_agency_sk) DESC,
         t.root_agency_name,
         t.depth,
         COALESCE(ab.won_value_zar, 0) DESC;


-- =====================================================================
-- Q14. AGENCY PERFORMANCE AS IT WAS, NOT AS IT IS
--
-- BUSINESS QUESTION: Which agencies were actually winning business in 2022?
--   Brokers have moved between agencies since, so who gets the credit?
-- TECHNIQUE: The payoff of the Type 2 dimension. The event fact stores the
--   broker_sk that was in force at the instant of the event, so joining
--   dim_broker on the surrogate key alone returns the HISTORIC agency for free,
--   with no date range predicate at query time. Joining through broker_nk to
--   the is_current row instead returns TODAY'S agency, which is what a Type 1
--   dimension would have silently given you and no one would have noticed.
--
--   THIS QUERY IS THE ARGUMENT FOR SCD2. It runs both attributions over exactly
--   the same 2022 wins and shows the disagreement in rows. If both columns
--   matched everywhere, the Type 2 dimension would be costing storage and
--   complexity for nothing. They do not match, so the misattribution column is
--   the measured cost of having got this wrong.
--
--   WHY THE JOIN NEEDS NO BETWEEN. Resolving the point in time key once at load
--   time is the highest leverage physical decision in the warehouse: this query
--   is a plain equality join on broker_sk, not a range join on
--   event_ts BETWEEN valid_from AND valid_to. The event_date filter also prunes
--   the fact to twelve monthly partitions out of eighty five.
-- =====================================================================
WITH wins_2022 AS (
    -- to_stage_sk 8 is Closed won. The literal is acceptable in a CTE this small
    -- because dim_stage is joined below to prove it, but a report that filters on
    -- stage should generally join dim_stage on is_won instead of naming a key.
    SELECT f.deal_nk,
           f.broker_sk,
           f.deal_value_zar
    FROM mart.fct_deal_stage_event f
    JOIN mart.dim_stage s ON s.stage_sk = f.to_stage_sk AND s.is_won
    WHERE f.event_date >= '2022-01-01'
      AND f.event_date <  '2023-01-01'
),
attributed AS (
    SELECT hist.agency_name AS agency_at_the_time,
           curr.agency_name AS agency_today,
           w.deal_value_zar
    FROM wins_2022 w
    -- Equality join on the surrogate key gives the 2022 version of the broker.
    JOIN mart.dim_broker hist ON hist.broker_sk = w.broker_sk
    -- Hopping back out through the natural key gives today's version.
    JOIN mart.dim_broker curr ON curr.broker_nk = hist.broker_nk
                             AND curr.is_current
),
as_it_was AS (
    SELECT agency_at_the_time AS agency_name,
           count(*)               AS wins,
           sum(deal_value_zar)    AS won_value_zar
    FROM attributed GROUP BY 1
),
as_it_is AS (
    SELECT agency_today AS agency_name,
           count(*)            AS wins,
           sum(deal_value_zar) AS won_value_zar
    FROM attributed GROUP BY 1
)
SELECT COALESCE(w.agency_name, i.agency_name)                        AS agency_name,
       COALESCE(w.wins, 0)                                           AS wins_credited_historically,
       COALESCE(i.wins, 0)                                           AS wins_credited_to_todays_org,
       COALESCE(i.wins, 0) - COALESCE(w.wins, 0)                     AS win_misattribution,
       round(COALESCE(w.won_value_zar, 0) / 1e9, 3)                  AS historic_value_zar_bn,
       round(COALESCE(i.won_value_zar, 0) / 1e9, 3)                  AS todays_org_value_zar_bn,
       round((COALESCE(i.won_value_zar, 0)
              - COALESCE(w.won_value_zar, 0)) / 1e9, 3)              AS value_misattribution_zar_bn,
       round(100.0 * (COALESCE(i.won_value_zar, 0) - COALESCE(w.won_value_zar, 0))
             / NULLIF(COALESCE(w.won_value_zar, 0), 0), 1)           AS misattribution_pct
-- FULL JOIN because an agency can appear on one side only: one that existed in
-- 2022 and has since lost every broker has no row today, and one that has only
-- recently acquired brokers has no 2022 row. An inner join would hide exactly
-- the cases that matter most.
FROM as_it_was w
FULL JOIN as_it_is i ON i.agency_name = w.agency_name
ORDER BY abs(COALESCE(i.wins, 0) - COALESCE(w.wins, 0)) DESC,
         COALESCE(w.agency_name, i.agency_name)
LIMIT 15;

\timing off
