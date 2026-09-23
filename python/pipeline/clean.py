import pandas as pd
import hashlib
from dataclasses import dataclass

REQUIRED_COLUMNS = [
    "event_id", "user_id", "app_id", "event_name", "event_time", "ingested_at", "country",
    "media_source", "campaign", "revenue_usd", "is_test"
]


@dataclass
class CleanResult:
    clean: pd.DataFrame
    quarantine: pd.DataFrame
    zero_revenue_count: int


def _parse_revenue(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "NULL": None, "null": None, "nan": None, "None": None})
    s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def _normalise_country(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip().str.upper()
    return s.where(s.str.match("^[A-Z]{2}$"), "XX")


def clean_events(raw: pd.DataFrame) -> CleanResult:
    df = raw.copy()

    missing_cols = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise ValueError(f"input is missing required columns: {missing_cols}")

    df["event_id"] = df["event_id"].astype(str).str.strip()
    df["app_id"] = df["app_id"].astype(str).str.strip()

    event_time = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    ingested_at = pd.to_datetime(df["ingested_at"], utc=True, errors="coerce")

    revenue_parsed = _parse_revenue(df["revenue_usd"])
    zero_revenue_count = int(revenue_parsed.isna().sum())
    revenue_final = revenue_parsed.fillna(0.0)

    country = _normalise_country(df["country"])

    is_test = (
        df["is_test"].astype(str).str.strip().str.lower() == "true"
    )

    working = df.assign(
        event_time=event_time,
        ingested_at=ingested_at,
        revenue_usd=revenue_final,
        country=country,
        is_test=is_test,
    )

    bad_mask = (
        working["event_id"].eq("")
            | working["app_id"].eq("")
            | working["event_time"].isna()
            | working["ingested_at"].isna()
    )
    line_source = df["_src_line"] if "_src_line" in df.columns else df.index + 2
    quarantine = pd.DataFrame({
        "_line": line_source[bad_mask],
        "_raw": df.loc[bad_mask, REQUIRED_COLUMNS].astype(str).agg(",".join, axis=1)
    })

    good = working.loc[~bad_mask].copy()
    good = good[~good["is_test"]]

    good["_tiebreak"] = _row_hash(good)
    good = good.sort_values(["ingested_at", "_tiebreak"], ascending=[False, False])
    clean = good.drop_duplicates(subset="event_id", keep="first").drop(columns="_tiebreak")
    clean = clean[REQUIRED_COLUMNS]

    return CleanResult(
        clean=clean.reset_index(drop=True),
        quarantine=quarantine.reset_index(drop=True),
        zero_revenue_count=zero_revenue_count
    )


def _row_hash(df: pd.DataFrame) -> pd.Series:
    cols = [c for c in REQUIRED_COLUMNS if c != "is_test"]
    concat = df[cols[0]].astype(str)
    for c in cols[1:]:
        concat = concat + "-" + df[c].astype(str)
    return concat.map(lambda s: hashlib.md5(s.encode()).hexdigest())