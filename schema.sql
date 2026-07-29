-- DealFlow SQL: schema for a commercial property deals database.
-- Three related tables: brokers -> deals <- properties.

DROP TABLE IF EXISTS deals;
DROP TABLE IF EXISTS properties;
DROP TABLE IF EXISTS brokers;

CREATE TABLE brokers (
  broker_id   serial PRIMARY KEY,
  full_name   text    NOT NULL,
  email       text    NOT NULL UNIQUE,
  phone       text,
  agency      text    NOT NULL,
  is_active   boolean NOT NULL DEFAULT true
);

CREATE TABLE properties (
  property_id   serial PRIMARY KEY,
  property_name text NOT NULL UNIQUE,
  common_name   text,
  sector        text NOT NULL,
  location      text NOT NULL,
  province      text NOT NULL
);

CREATE TABLE deals (
  deal_id        serial PRIMARY KEY,
  message_id     text    NOT NULL UNIQUE,
  subject        text    NOT NULL,
  summary        text,
  broker_id      integer NOT NULL REFERENCES brokers (broker_id),
  property_id    integer NOT NULL REFERENCES properties (property_id),
  deal_value_zar numeric(14, 2) NOT NULL DEFAULT 0 CHECK (deal_value_zar >= 0),
  status         text    NOT NULL CHECK (status IN ('New', 'In review', 'Qualified', 'Declined', 'Closed')),
  needs_review   boolean NOT NULL DEFAULT false,
  review_reason  text,
  received_at    timestamp NOT NULL
);

CREATE INDEX idx_deals_broker_id ON deals (broker_id);
CREATE INDEX idx_deals_property_id ON deals (property_id);
CREATE INDEX idx_deals_status ON deals (status);
