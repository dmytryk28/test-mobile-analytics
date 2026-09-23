## 1. Data

Generated data:

apps: 5, costs: 4,049, events: 97,451 (94,535 distinct), staging: 650

Assumed that this data meets the task requirements.

## 2.1 - De-duplication

Dedup tie-break (same event_id, same ingested_at) uses a row hash.
Only guarantees the pick is deterministic.

## 2.2 - Window functions

Day-over-day % change is NULL when the previous day has no revenue or doesn't exist.

## 2.3 - Joining costs

ROAS is NULL when cost is 0 or missing.

Assumes campaign_costs.csv has at most one row per key.

## 2.4 - Incremental load

The MERGE processes the entire staging_events table with no time-window filter.
MATCHED/NOT MATCHED make old rows already in clean_events a no-op,
not a duplicate. If staging is never trimmed, every run rescans more weight.
Trimming it too aggressively could drop a late correction before it's loaded.

## 3.1 - Cleaning

Money is stored as NUMERIC to avoid rounding drift.

is_test is a string in raw data, cast to bool, not compared directly.

Unparseable event_time or ingested_at are treated as a bad row and excluded.

revenue_usd that's blank, "NULL", unparseable becomes 0.0 and counted separately.

Negative revenue is kept as-is (assumed to be a valid refund/adjustment).

The dataset is assumed not to contain full country names, only 2-letter codes.
A full name would also become "XX".

## 3.2 - Pipeline

Rows with the wrong number of CSV fields are quarantined before parsing,
separate from rows that parse but fail a business rule. Both go to the same quarantine
file: line number + raw row, so quarantine.csv has one consistent shape.

## Not finished
- Task 2.5
- Star schema (4.1) is described, not implemented as image/scheme.