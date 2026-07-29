# DealFlow SQL

A small but real PostgreSQL project: a commercial property deal intake database
(brokers email in deals on properties, deals carry a value, a status, and review flags).

All data is synthetic seed data written for this project: 6 invented brokers, 10 invented
properties, 15 invented deals. No real broker, client, deal or company data appears
anywhere in this repository.

Everything below is reproducible in one command. Every query result in this README is
the actual output of running `queries.sql` against the loaded database.

## Stack

- PostgreSQL 16 in Docker (container `dealflow-db`, host port 5433)
- Plain SQL: `schema.sql`, `load.sql` (CSV staging load with FK resolution), `queries.sql`
- `run.sh`: start container, apply schema, load CSVs, run all 10 queries

## Run it

```
./run.sh          # start + schema + load + all 10 queries
./run.sh stop     # stop the container (data kept)
docker start dealflow-db   # restart it later
./run.sh clean    # remove the container entirely
```

The database password lives only in a local `.env` (gitignored). `run.sh` generates one
on first run. Connect directly with:

```
psql "postgresql://dealflow:<password from .env>@localhost:5433/dealflow"
```

## Schema

Three related tables. `deals` is the fact table; `brokers` and `properties` are lookups.

```
+--------------------+          +----------------------+
|      brokers       |          |      properties      |
+--------------------+          +----------------------+
| broker_id      PK  |          | property_id      PK  |
| full_name          |          | property_name    UQ  |
| email          UQ  |          | common_name          |
| phone              |          | sector               |
| agency             |          | location             |
| is_active          |          | province             |
+---------+----------+          +----------+-----------+
          |                                |
          | 1                              | 1
          |                                |
          | *                              | *
+---------+--------------------------------+-----------+
|                        deals                         |
+------------------------------------------------------+
| deal_id         PK                                   |
| message_id      UQ   (email Message ID, dedupe key)  |
| subject                                              |
| summary                                              |
| broker_id       FK -> brokers.broker_id              |
| property_id     FK -> properties.property_id         |
| deal_value_zar  numeric(14,2), CHECK >= 0            |
| status          CHECK (New / In review / Qualified   |
|                        / Declined / Closed)          |
| needs_review    boolean                              |
| review_reason                                        |
| received_at     timestamp                            |
+------------------------------------------------------+
```

Load pattern in `load.sql`: `\copy` each CSV into a temp staging table of text columns,
then `INSERT ... SELECT` into the real tables, resolving foreign keys by joining on
broker email and property name, casting types on the way in. The whole load runs in one
transaction with `ON_ERROR_STOP`.

Data note: one row in the source `deals.csv` (MSG-0012) had an unquoted comma inside the
summary field, which broke the CSV into 11 columns. The copy in `data/deals.csv` fixes
that row by quoting the field. The original seed file is untouched.

## The 10 queries

"Open pipeline" throughout means status in New, In review, or Qualified.

### Q1. SELECT / WHERE / ORDER BY

The live pipeline: every open deal with a stated value, biggest first.

```
               subject               |  status   | deal_value_zar 
-------------------------------------+-----------+----------------
 Century City mixed use block        | New       |    96000000.00
 Kempton Park land rezoning          | In review |    55000000.00
 Montague Gardens warehouse disposal | In review |    42000000.00
 Northgate anchor tenant sale        | In review |    41200000.00
 Riverside Heights bulk sale         | In review |    33800000.00
 Sandton Exchange floor 9            | New       |    31000000.00
 Paarden Eiland depot mandate        | Qualified |    27500000.00
 Harbour sheds portfolio             | Qualified |    23400000.00
 Foreshore sublease opportunity      | New       |    18500000.00
 Montague Park expansion erf         | New       |    12800000.00
 Harbour sheds tenant buyout         | New       |     9800000.00
 Northgate parking lease             | Qualified |     4600000.00
(12 rows)
```

### Q2. WHERE with multiple conditions

The review queue: deals flagged for a human, with the reason, newest first.

```
            subject            |             review_reason             | deal_value_zar |     received_at     
-------------------------------+---------------------------------------+----------------+---------------------
 Foreshore naming rights query | No deal value stated in mail          |           0.00 | 2026-07-17 13:27:00
 Riverside Heights bulk sale   | Deal value above the review threshold |    33800000.00 | 2026-07-12 15:08:00
 Kempton Park land rezoning    | Rezoning risk not yet verified        |    55000000.00 | 2026-07-09 08:55:00
 Northgate anchor tenant sale  | Deal value above the review threshold |    41200000.00 | 2026-07-03 11:02:00
(4 rows)
```

### Q3. INNER JOIN

Each deal with the sector and province of the property it concerns.

```
               subject               |   sector    |   province    | deal_value_zar 
-------------------------------------+-------------+---------------+----------------
 Montague Gardens warehouse disposal | Industrial  | Western Cape  |    42000000.00
 Foreshore sublease opportunity      | Office      | Western Cape  |    18500000.00
 Northgate anchor tenant sale        | Retail      | Gauteng       |    41200000.00
 Paarden Eiland depot mandate        | Industrial  | Western Cape  |    27500000.00
 Sandton Exchange floor 9            | Office      | Gauteng       |    31000000.00
 Ridge Plaza food court rights       | Retail      | KwaZulu-Natal |     8200000.00
 Kempton Park land rezoning          | Land        | Gauteng       |    55000000.00
 Century City mixed use block        | Mixed use   | Western Cape  |    96000000.00
 Harbour sheds portfolio             | Industrial  | Eastern Cape  |    23400000.00
 Riverside Heights bulk sale         | Residential | Gauteng       |    33800000.00
 Montague Park expansion erf         | Industrial  | Western Cape  |    12800000.00
 Foreshore naming rights query       | Office      | Western Cape  |           0.00
 Northgate parking lease             | Retail      | Gauteng       |     4600000.00
 Exchange Building auction notice    | Office      | Gauteng       |           0.00
 Harbour sheds tenant buyout         | Industrial  | Eastern Cape  |     9800000.00
(15 rows)
```

### Q4. Two INNER JOINs

The full deal sheet: which broker brought which deal on which property.

```
 message_id |     broker     |         agency          |        property         | deal_value_zar |  status   
------------+----------------+-------------------------+-------------------------+----------------+-----------
 MSG-0001   | Thandi Mokoena | Atlantic Properties     | Montague Park Warehouse |    42000000.00 | In review
 MSG-0002   | Nolan Fourie   | Cape Commercial Brokers | Foreshore Offices       |    18500000.00 | New
 MSG-0003   | Sarah Naidoo   | Metro Realty Group      | Northgate Centre        |    41200000.00 | In review
 MSG-0004   | James Botha    | Southpoint Estates      | Paarden Eiland Depot    |    27500000.00 | Qualified
 MSG-0005   | Lerato Dlamini | Urban Yield Capital     | Exchange Building       |    31000000.00 | New
 MSG-0006   | Sarah Naidoo   | Metro Realty Group      | Ridge Plaza             |     8200000.00 | Declined
 MSG-0007   | Nolan Fourie   | Cape Commercial Brokers | Kempton Land Parcel     |    55000000.00 | In review
 MSG-0008   | Thandi Mokoena | Atlantic Properties     | Century City Annex      |    96000000.00 | New
 MSG-0009   | James Botha    | Southpoint Estates      | Harbour Sheds           |    23400000.00 | Qualified
 MSG-0010   | Lerato Dlamini | Urban Yield Capital     | Riverside Heights       |    33800000.00 | In review
 MSG-0011   | Thandi Mokoena | Atlantic Properties     | Montague Park Warehouse |    12800000.00 | New
 MSG-0012   | Nolan Fourie   | Cape Commercial Brokers | Foreshore Offices       |           0.00 | New
 MSG-0013   | Sarah Naidoo   | Metro Realty Group      | Northgate Centre        |     4600000.00 | Qualified
 MSG-0014   | Lerato Dlamini | Urban Yield Capital     | Exchange Building       |           0.00 | Closed
 MSG-0015   | James Botha    | Southpoint Estates      | Harbour Sheds           |     9800000.00 | New
(15 rows)
```

### Q5. LEFT JOIN + GROUP BY

Broker coverage: every broker on the books, including the one with zero deals.

```
    full_name     |         agency          | is_active | deal_count | total_value_zar 
------------------+-------------------------+-----------+------------+-----------------
 James Botha      | Southpoint Estates      | t         |          3 |     60700000.00
 Lerato Dlamini   | Urban Yield Capital     | t         |          3 |     64800000.00
 Nolan Fourie     | Cape Commercial Brokers | t         |          3 |     73500000.00
 Sarah Naidoo     | Metro Realty Group      | t         |          3 |     54000000.00
 Thandi Mokoena   | Atlantic Properties     | t         |          3 |    150800000.00
 Michael Abrahams | Peninsula Property Co   | f         |          0 |               0
(6 rows)
```

### Q6. GROUP BY

Open pipeline value by property sector.

```
   sector    | open_deals | pipeline_zar | avg_deal_zar 
-------------+------------+--------------+--------------
 Industrial  |          5 | 115500000.00 |     23100000
 Mixed use   |          1 |  96000000.00 |     96000000
 Land        |          1 |  55000000.00 |     55000000
 Office      |          3 |  49500000.00 |     16500000
 Retail      |          2 |  45800000.00 |     22900000
 Residential |          1 |  33800000.00 |     33800000
(6 rows)
```

### Q7. GROUP BY + HAVING

Agencies whose open pipeline exceeds R30M.

```
         agency          | open_deals | pipeline_zar 
-------------------------+------------+--------------
 Atlantic Properties     |          3 | 150800000.00
 Cape Commercial Brokers |          3 |  73500000.00
 Urban Yield Capital     |          2 |  64800000.00
 Southpoint Estates      |          3 |  60700000.00
 Metro Realty Group      |          2 |  45800000.00
(5 rows)
```

### Q8. GROUP BY on date_trunc

Weekly deal flow: deals and value received per week (swap week for month as the data grows).

```
 week_starting | deals_received | value_received_zar 
---------------+----------------+--------------------
 2026-06-29    |              4 |       129200000.00
 2026-07-06    |              6 |       247400000.00
 2026-07-13    |              3 |        17400000.00
 2026-07-20    |              2 |         9800000.00
(4 rows)
```

### Q9. Subquery with NOT EXISTS

Vacancy-style view: properties with no open deal against them.

```
 common_name | sector |   province    
-------------+--------+---------------
 Ridge Plaza | Retail | KwaZulu-Natal
(1 row)
```

### Q10. Window functions (RANK, SUM OVER)

Broker leaderboard: rank by open pipeline plus each broker's share of the whole book.

```
 rank |   full_name    |         agency          | pipeline_zar | pct_of_book 
------+----------------+-------------------------+--------------+-------------
    1 | Thandi Mokoena | Atlantic Properties     | 150800000.00 |        38.1
    2 | Nolan Fourie   | Cape Commercial Brokers |  73500000.00 |        18.6
    3 | Lerato Dlamini | Urban Yield Capital     |  64800000.00 |        16.4
    4 | James Botha    | Southpoint Estates      |  60700000.00 |        15.3
    5 | Sarah Naidoo   | Metro Realty Group      |  45800000.00 |        11.6
(5 rows)
```

## What the outputs say

- The open book is R395.6M across 13 open deals, 12 of which carry a stated value; the two
  deals flagged on value account for R75.0M of it (Q1, Q2, Q10).
- Industrial leads on both open value (R115.5M) and volume (5 open deals), while a single
  Mixed use deal at R96.0M is the largest line on the book (Q6).
- One broker (Michael Abrahams, inactive) has zero deals, which is exactly what the
  LEFT JOIN in Q5 exists to show.
- Ridge Plaza is the only property with nothing live against it (Q9).
- Deal flow peaked in the week of 6 July (6 deals, R247.4M) and tailed off after (Q8).
