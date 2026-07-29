-- DealFlow SQL: 10 analysis queries, simplest to most advanced.
-- "Open pipeline" throughout = status in ('New', 'In review', 'Qualified').

-- Q1. SELECT / WHERE / ORDER BY
-- Top open deals by value: the live pipeline, biggest first.
SELECT subject, status, deal_value_zar
FROM deals
WHERE status IN ('New', 'In review', 'Qualified')
  AND deal_value_zar > 0
ORDER BY deal_value_zar DESC;

-- Q2. WHERE with multiple conditions + date ordering
-- Review queue: flagged deals with their reasons, newest first.
SELECT subject, review_reason, deal_value_zar, received_at
FROM deals
WHERE needs_review = true
ORDER BY received_at DESC;

-- Q3. INNER JOIN
-- Deals with the sector and province of the property they concern.
SELECT d.subject, p.sector, p.province, d.deal_value_zar
FROM deals d
JOIN properties p ON p.property_id = d.property_id
ORDER BY d.received_at;

-- Q4. Two INNER JOINs
-- Full deal sheet: who brought which deal on which property.
SELECT d.message_id, b.full_name AS broker, b.agency,
       p.common_name AS property, d.deal_value_zar, d.status
FROM deals d
JOIN brokers b ON b.broker_id = d.broker_id
JOIN properties p ON p.property_id = d.property_id
ORDER BY d.message_id;

-- Q5. LEFT JOIN
-- Broker coverage: every broker, including those with zero deals.
SELECT b.full_name, b.agency, b.is_active,
       count(d.deal_id) AS deal_count,
       coalesce(sum(d.deal_value_zar), 0) AS total_value_zar
FROM brokers b
LEFT JOIN deals d ON d.broker_id = b.broker_id
GROUP BY b.broker_id, b.full_name, b.agency, b.is_active
ORDER BY deal_count DESC, b.full_name;

-- Q6. GROUP BY (aggregate)
-- Open pipeline value by property sector.
SELECT p.sector,
       count(*) AS open_deals,
       sum(d.deal_value_zar) AS pipeline_zar,
       round(avg(d.deal_value_zar), 0) AS avg_deal_zar
FROM deals d
JOIN properties p ON p.property_id = d.property_id
WHERE d.status IN ('New', 'In review', 'Qualified')
GROUP BY p.sector
ORDER BY pipeline_zar DESC;

-- Q7. GROUP BY + HAVING
-- Agencies whose open pipeline exceeds R30M.
SELECT b.agency,
       count(*) AS open_deals,
       sum(d.deal_value_zar) AS pipeline_zar
FROM deals d
JOIN brokers b ON b.broker_id = d.broker_id
WHERE d.status IN ('New', 'In review', 'Qualified')
GROUP BY b.agency
HAVING sum(d.deal_value_zar) > 30000000
ORDER BY pipeline_zar DESC;

-- Q8. GROUP BY on a time bucket (date_trunc)
-- Weekly deal flow: deals received and value per week.
-- Same pattern with date_trunc('month', ...) gives monthly deal flow
-- once the data spans more than one month.
SELECT date_trunc('week', received_at)::date AS week_starting,
       count(*) AS deals_received,
       sum(deal_value_zar) AS value_received_zar
FROM deals
GROUP BY week_starting
ORDER BY week_starting;

-- Q9. Subquery (NOT EXISTS)
-- Vacancy-style view: properties with no open deal against them,
-- i.e. stock on the books that nothing live is happening on.
SELECT p.common_name, p.sector, p.province
FROM properties p
WHERE NOT EXISTS (
  SELECT 1
  FROM deals d
  WHERE d.property_id = p.property_id
    AND d.status IN ('New', 'In review', 'Qualified')
)
ORDER BY p.sector, p.common_name;

-- Q10. Window functions (RANK + share of total)
-- Broker leaderboard: rank by open pipeline and share of the whole book.
SELECT rank() OVER (ORDER BY sum(d.deal_value_zar) DESC) AS rank,
       b.full_name,
       b.agency,
       sum(d.deal_value_zar) AS pipeline_zar,
       round(100.0 * sum(d.deal_value_zar)
             / sum(sum(d.deal_value_zar)) OVER (), 1) AS pct_of_book
FROM deals d
JOIN brokers b ON b.broker_id = d.broker_id
WHERE d.status IN ('New', 'In review', 'Qualified')
GROUP BY b.broker_id, b.full_name, b.agency
ORDER BY rank;
