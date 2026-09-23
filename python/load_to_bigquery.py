"""
Loads apps.csv, campaign_costs.csv, events_raw.csv, staging_events.csv
from ./data into BigQuery, dataset `raw` (created if missing).
Table name == csv file stem.

Connection config comes from a local .env file — see the note at the
bottom of this file for what goes in it and where to get the values.

Usage: python load_to_bigquery.py
Requires: pip install google-cloud-bigquery python-dotenv pandas pyarrow
"""
from __future__ import annotations
import csv
import logging
import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("load_to_bigquery")

DATA_DIR = Path("data")
ENV_PATH = Path(__file__).resolve().parent / ".env"

# ---- Schemas ----------------------------------------------------------

SCHEMAS: dict[str, list[bigquery.SchemaField]] = {
    "apps": [
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("app_name", "STRING"),
        bigquery.SchemaField("platform", "STRING"),
        bigquery.SchemaField("store_id", "STRING"),
        bigquery.SchemaField("launched_on", "DATE"),
    ],
    "campaign_costs": [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("media_source", "STRING"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("cost_usd", "STRING"),
        bigquery.SchemaField("impressions", "INT64"),
        bigquery.SchemaField("clicks", "INT64"),
    ],
    "events_raw": [
        bigquery.SchemaField("event_id", "STRING"),
        bigquery.SchemaField("user_id", "STRING"),
        bigquery.SchemaField("app_id", "STRING"),
        bigquery.SchemaField("event_name", "STRING"),
        bigquery.SchemaField("event_time", "STRING"),
        bigquery.SchemaField("ingested_at", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("media_source", "STRING"),
        bigquery.SchemaField("campaign", "STRING"),
        bigquery.SchemaField("revenue_usd", "STRING"),
        bigquery.SchemaField("is_test", "STRING"),
    ],
}
SCHEMAS["staging_events"] = SCHEMAS["events_raw"]  # same grain and shape

REQUIRED_FIELDS: dict[str, set[str]] = {
    "apps": {"app_id", "app_name", "platform", "store_id", "launched_on"},
    "campaign_costs": {"date", "app_id", "media_source", "campaign"},
    "events_raw": {"event_id", "app_id"},
    "staging_events": {"event_id", "app_id"},
}

TABLE_FILES = {
    "apps": "apps.csv",
    "campaign_costs": "campaign_costs.csv",
    "events_raw": "events_raw.csv",
    "staging_events": "staging_events.csv",
}

TYPED_FIELDS = {"DATE", "FLOAT64", "INT64"}  # fields we coerce before upload


# ---- Structural validation ---------------------------------------------
def validate_rows(path: Path, table_name: str) -> tuple[pd.DataFrame, list[dict]]:
    """
    Row-level check against the schema, done BEFORE upload:
      - correct number of fields (this is what catches the ragged/malformed
        rows the generator seeds on purpose),
      - required fields non-empty,
      - typed fields (DATE / FLOAT64 / INT64) actually parse.
    Returns (valid_rows_df, quarantined_rows) -- bad rows never reach BigQuery.
    """
    schema = SCHEMAS[table_name]
    columns = [f.name for f in schema]
    required = REQUIRED_FIELDS.get(table_name, set())
    typed = {f.name: f.field_type for f in schema if f.field_type in TYPED_FIELDS}

    good_rows, bad_rows = [], []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != columns:
            raise ValueError(f"{path.name}: header {header} != expected schema {columns}")

        for line_no, raw in enumerate(reader, start=2):
            if len(raw) != len(columns):
                bad_rows.append({"line": line_no, "raw": raw,
                                  "reason": f"expected {len(columns)} fields, got {len(raw)}"})
                continue

            record = dict(zip(columns, raw))
            missing = [c for c in required if record.get(c, "") == ""]
            if missing:
                bad_rows.append({"line": line_no, "raw": raw, "reason": f"missing required: {missing}"})
                continue

            type_error = None
            for col, bq_type in typed.items():
                val = record[col]
                try:
                    if bq_type == "DATE":
                        pd.Timestamp(val)
                    elif bq_type in ("FLOAT64", "INT64"):
                        float(val)
                except (ValueError, TypeError):
                    type_error = f"{col} is not a valid {bq_type}: {val!r}"
                    break
            if type_error:
                bad_rows.append({"line": line_no, "raw": raw, "reason": type_error})
                continue

            good_rows.append(record)

    df = pd.DataFrame(good_rows, columns=columns)
    for col, bq_type in typed.items():
        if bq_type == "DATE":
            df[col] = pd.to_datetime(df[col]).dt.date
        elif bq_type == "FLOAT64":
            df[col] = df[col].astype(float)
        elif bq_type == "INT64":
            df[col] = df[col].astype("int64")
    return df, bad_rows


# ---- BigQuery -------------------------------------------------------------
def get_client() -> bigquery.Client:
    return bigquery.Client(project=os.environ["GCP_PROJECT_ID"])


def ensure_dataset(client: bigquery.Client, dataset_id: str, location: str) -> None:
    ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(ref)
    except NotFound:
        ds = bigquery.Dataset(ref)
        ds.location = location
        client.create_dataset(ds)
        log.info("created dataset %s.%s in %s", client.project, dataset_id, location)


def load_table(client: bigquery.Client, dataset_id: str, table_name: str, df: pd.DataFrame) -> None:
    table_ref = f"{client.project}.{dataset_id}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        schema=SCHEMAS[table_name],
        write_disposition="WRITE_TRUNCATE",  # full refresh each run -- fine for a raw/staging mirror;
    )                                        # swap to WRITE_APPEND if you want history kept in BQ itself.
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    log.info("loaded %d row(s) into %s", len(df), table_ref)


def main() -> None:
    if not ENV_PATH.exists():
        sys.exit(f".env not found at {ENV_PATH} -- see the header comment for what to put in it.")
    load_dotenv(ENV_PATH)

    dataset_id = os.environ.get("GCP_DATASET", "raw")
    location = os.environ.get("BQ_LOCATION", "US")

    client = get_client()
    ensure_dataset(client, dataset_id, location)

    for table_name, filename in TABLE_FILES.items():
        path = DATA_DIR / filename
        if not path.exists():
            log.warning("skipping %s: %s not found", table_name, path)
            continue

        valid_df, bad_rows = validate_rows(path, table_name)
        if bad_rows:
            quarantine_path = DATA_DIR / f"_quarantine_{table_name}.csv"
            pd.DataFrame(bad_rows).to_csv(quarantine_path, index=False)
            log.warning("%s: %d row(s) failed schema validation -> %s",
                        table_name, len(bad_rows), quarantine_path)

        load_table(client, dataset_id, table_name, valid_df)

    log.info("done: %d table(s) processed", len(TABLE_FILES))


if __name__ == "__main__":
    main()