-- DealFlow SQL: load the seed CSVs into the normalized schema.
-- CSVs are mounted into the container at /data (see run.sh).
-- Pattern: copy raw CSV rows into temp staging tables, then insert into the
-- real tables, resolving foreign keys by broker email and property name.

\set ON_ERROR_STOP on

BEGIN;

TRUNCATE deals, properties, brokers RESTART IDENTITY CASCADE;

-- 1. Brokers
CREATE TEMP TABLE stg_brokers (
  broker_name text,
  email       text,
  phone       text,
  agency      text,
  active      text
) ON COMMIT DROP;

\copy stg_brokers FROM '/data/brokers.csv' WITH (FORMAT csv, HEADER true)

INSERT INTO brokers (full_name, email, phone, agency, is_active)
SELECT broker_name, email, phone, agency, active = 'Yes'
FROM stg_brokers;

-- 2. Properties
CREATE TEMP TABLE stg_properties (
  property_name text,
  common_name   text,
  sector        text,
  location      text,
  province      text
) ON COMMIT DROP;

\copy stg_properties FROM '/data/properties.csv' WITH (FORMAT csv, HEADER true)

INSERT INTO properties (property_name, common_name, sector, location, province)
SELECT property_name, common_name, sector, location, province
FROM stg_properties;

-- 3. Deals (FKs resolved by joining on broker email and property name)
CREATE TEMP TABLE stg_deals (
  subject        text,
  summary        text,
  broker_email   text,
  property_name  text,
  deal_value_zar text,
  status         text,
  needs_review   text,
  review_reason  text,
  message_id     text,
  received_date  text
) ON COMMIT DROP;

\copy stg_deals FROM '/data/deals.csv' WITH (FORMAT csv, HEADER true)

INSERT INTO deals (message_id, subject, summary, broker_id, property_id,
                   deal_value_zar, status, needs_review, review_reason, received_at)
SELECT
  s.message_id,
  s.subject,
  s.summary,
  b.broker_id,
  p.property_id,
  s.deal_value_zar::numeric,
  s.status,
  s.needs_review = 'Yes',
  NULLIF(s.review_reason, ''),
  s.received_date::timestamp
FROM stg_deals s
JOIN brokers b ON b.email = s.broker_email
JOIN properties p ON p.property_name = s.property_name;

COMMIT;

SELECT
  (SELECT count(*) FROM brokers)    AS brokers_loaded,
  (SELECT count(*) FROM properties) AS properties_loaded,
  (SELECT count(*) FROM deals)      AS deals_loaded;
