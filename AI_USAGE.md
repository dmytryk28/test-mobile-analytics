Tool used: Claude. Everything below was reviewed and manually corrected before being included.

## 1. Data (generate_data.py, load_to_bigquery.py)

The synthetic data generator and the BigQuery load script were largely written with Claude.

## 2.1. De-duplication

The idea of FARM_FINGERPRINT() tie-break. I saw that the second sort key was needed, but could not choose it.

## 2.2. Window functions

The calendar CTE code. I identified the missing-days problem and told AI what fields and behavior
I needed (app_id, event_date, filling gaps with zero revenue).

## 2.4. Incremental load

MATCHED/NOT MATCHED MERGE structure - update only if the incoming row is newer, insert otherwise.

## 3.1. Cleaning

The _row_hash tie-break function, same idea as 2.1 applied to the pandas implementation.

## ANSWERS.md

All thoughts and conclusions are mine, but I used Claude to structure some sentences more clearly.