# python -m pipeline.run --input data/ --output out/ --since 2026-01-01

import csv
import pandas as pd
import argparse
import shutil
from pathlib import Path
from pandas import DataFrame
from pipeline.clean import clean_events, REQUIRED_COLUMNS


def _load_raw(input_dir: Path) -> tuple[DataFrame, DataFrame]:
    csv_path = input_dir / "events_raw.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"{csv_path} not exist")

    good_rows, bad_rows = [], []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader)
        if header != REQUIRED_COLUMNS:
            raise ValueError(f"unexpected header in {csv_path}: {header}")

        for line_no, fields in enumerate(reader, start=2):
            if len(fields) != len(REQUIRED_COLUMNS):
                bad_rows.append({"_line": line_no, "_raw": ",".join(fields)})
                continue
            record = dict(zip(REQUIRED_COLUMNS, fields))
            record["_src_line"] = line_no
            good_rows.append(record)

    raw = pd.DataFrame(good_rows, columns=REQUIRED_COLUMNS + ["_src_line"])
    raw[REQUIRED_COLUMNS] = raw[REQUIRED_COLUMNS].astype(str)
    structural_quarantine = pd.DataFrame(bad_rows, columns=["_line", "_raw"])
    return raw, structural_quarantine


def run(input_dir: Path, output_dir: Path, since: str) -> None:
    raw, structural_quarantine = _load_raw(input_dir)
    rows_in = len(raw) + len(structural_quarantine)

    result = clean_events(raw)
    clean = result.clean

    since_ts = pd.Timestamp(since, tz="UTC")
    clean = clean[clean["event_time"] >= since_ts]

    events_dir = output_dir / "events"
    quarantine_dir = output_dir / "quarantine"
    events_dir.mkdir(parents=True, exist_ok=True)
    quarantine_dir.mkdir(parents=True, exist_ok=True)

    clean = clean.assign(event_date=clean["event_time"].dt.date.astype(str))

    for date, part in clean.groupby("event_date"):
        part_dir = events_dir / f"event_date_{date}"
        if part_dir.exists():
            shutil.rmtree(part_dir)
        part_dir.mkdir(parents=True, exist_ok=True)
        part.drop(columns="event_date").to_parquet(
            part_dir / "part-0.parquet", index=False
        )

    quarantine = pd.concat(
        [structural_quarantine, result.quarantine], ignore_index=True, sort=False
    )
    quarantine_path = quarantine_dir / "quarantine.csv"
    quarantine.to_csv(quarantine_path, index=False)

    rows_out = len(clean)
    duplicates_removed = (
        rows_in - len(result.clean) - len(result.quarantine) - len(structural_quarantine)
    )
    print(
        f"rows_in={rows_in}\nrows_out={rows_out}\n"
        f"duplicates_removed={duplicates_removed}\n"
        f"rows_quarantined={len(quarantine)}\n"
        f"zero_revenue_forced={result.zero_revenue_count}"
    )


def main(arg=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--since", required=True)
    args = parser.parse_args(arg)
    run(args.input, args.output, args.since)


if __name__ == "__main__":
    main()