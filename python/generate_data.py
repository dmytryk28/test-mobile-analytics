"""
Synthetic data generator -- events_raw.csv, apps.csv, campaign_costs.csv,
staging_events.csv at realistic volume, with the same edge cases the task
names explicitly, plus structurally malformed rows (wrong field count).
Standard library + pandas only. Deterministic given SEED.

Usage: python generate_data.py   # writes ./data/*.csv
"""
from __future__ import annotations
import csv
import random
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import pandas as pd

SEED = 42
OUTPUT_DIR = Path("data")
NUM_DAYS = 100
APPS = [  # app_id, platform, launch offset (days)
    ("app1", "ios", 0), ("app2", "android", 10), ("app3", "ios", 25),
    ("app4", "android", 40), ("app5", "ios", 5),
]
EVENTS_PER_APP_DAY = (50, 400)
MEDIA_SOURCES = ["google", "facebook", "tiktok", "applovin", "unity"]
CAMPAIGNS_PER_SOURCE = 3
ORGANIC_SHARE = 0.12
COUNTRIES = ["US", "GB", "DE", "FR", "BR", "IN", "JP", "CA", "AU", "NL"]

DUPLICATE_RATE = 0.03
DUPLICATE_TIE_RATE = 0.10        # of those duplicates, exact-same ingested_at
LATE_ARRIVAL_RATE = 0.02         # ingested_at 1-6 days after event_time
TEST_TRAFFIC_RATE = 0.015
MALFORMED_RATE = 0.003           # unparseable event_time value (existing edge case)
STRUCTURAL_MALFORMED_RATE = 0.004  # NEW: wrong number of CSV fields (too few / too many)
BLANK_REVENUE_RATE = 0.02
NULL_STRING_REVENUE_RATE = 0.015
COMMA_DECIMAL_REVENUE_RATE = 0.05
COUNTRY_MESSY_RATE = 0.06

STAGING_NEW_EVENTS = 500          # NEW: brand-new events for the next incremental load
STAGING_CORRECTIONS = 150         # NEW: late corrections to already-loaded event_ids

random.seed(SEED)

def _range_start() -> date:
    return date(2026, 1, 1)

def gen_apps() -> pd.DataFrame:
    start = _range_start()
    return pd.DataFrame([
        {"app_id": app_id, "app_name": f"Sample App {i+1}", "platform": platform,
         "store_id": f"{100000+i}", "launched_on": (start + timedelta(days=off)).isoformat()}
        for i, (app_id, platform, off) in enumerate(APPS)
    ])

def _campaign_catalog():
    return [(ms, f"camp_{ms}_{n}") for ms in MEDIA_SOURCES for n in range(CAMPAIGNS_PER_SOURCE)]

def gen_campaign_costs(apps: pd.DataFrame) -> pd.DataFrame:
    start, catalog, rows = _range_start(), _campaign_catalog(), []
    for _, app in apps.iterrows():
        launch = date.fromisoformat(app["launched_on"])
        for media_source, campaign in random.sample(catalog, k=random.randint(4, len(catalog))):
            d = launch
            while d < start + timedelta(days=NUM_DAYS):
                if random.random() < 0.15:   # real gaps, not just zero-cost
                    d += timedelta(days=1); continue
                impressions = random.randint(500, 20000)
                cost = 0.0 if random.random() < 0.08 else round(random.uniform(5, 500), 2)
                rows.append({"date": d.isoformat(), "app_id": app["app_id"],
                             "media_source": media_source, "campaign": campaign,
                             "cost_usd": cost, "impressions": impressions,
                             "clicks": int(impressions * random.uniform(0.01, 0.08))})
                d += timedelta(days=1)
    return pd.DataFrame(rows)

def _messy_revenue(value: float) -> str:
    r = random.random()
    if r < BLANK_REVENUE_RATE: return ""
    if r < BLANK_REVENUE_RATE + NULL_STRING_REVENUE_RATE: return "NULL"
    if r < BLANK_REVENUE_RATE + NULL_STRING_REVENUE_RATE + COMMA_DECIMAL_REVENUE_RATE:
        return f"{value:.2f}".replace(".", ",")
    return f"{value:.2f}"

def _messy_country(code: str) -> str:
    r = random.random()
    if r < COUNTRY_MESSY_RATE/3: return ""
    if r < COUNTRY_MESSY_RATE*2/3: return "--"
    if r < COUNTRY_MESSY_RATE: return code.lower()
    return code

def gen_events_raw(apps: pd.DataFrame, costs: pd.DataFrame) -> pd.DataFrame:
    start, catalog = _range_start(), _campaign_catalog()
    costed_pairs = costs.groupby("app_id")[["media_source","campaign"]] \
        .apply(lambda d: list(map(tuple, d.values))).to_dict()
    rows, seq = [], 0
    for _, app in apps.iterrows():
        launch = date.fromisoformat(app["launched_on"])
        pairs = costed_pairs.get(app["app_id"], [])
        d = launch
        while d < start + timedelta(days=NUM_DAYS):
            for _ in range(random.randint(*EVENTS_PER_APP_DAY)):
                seq += 1
                media_source, campaign = random.choice(pairs) if pairs and random.random() > ORGANIC_SHARE else random.choice(catalog)
                event_dt = datetime(d.year, d.month, d.day, random.randint(0,23),
                                     random.randint(0,59), random.randint(0,59), tzinfo=timezone.utc)
                ingested_dt = (event_dt + timedelta(days=random.randint(1,6), hours=random.randint(0,23))
                               if random.random() < LATE_ARRIVAL_RATE
                               else event_dt + timedelta(minutes=random.randint(0,240)))
                revenue_val = round(max(0.0, random.gauss(2.0, 3.0)), 2) if random.random() < 0.3 else 0.0
                event_time_str = event_dt.strftime("%Y-%m-%d %H:%M:%S")
                if random.random() < MALFORMED_RATE:
                    event_time_str = "not-a-timestamp"
                row = {"event_id": f"e{seq:08d}", "user_id": f"u{random.randint(1,50000)}",
                       "app_id": app["app_id"],
                       "event_name": random.choice(["impression","click","install","purchase"]),
                       "event_time": event_time_str,
                       "ingested_at": ingested_dt.strftime("%Y-%m-%d %H:%M:%S"),
                       "country": _messy_country(random.choice(COUNTRIES)),
                       "media_source": media_source, "campaign": campaign,
                       "revenue_usd": _messy_revenue(revenue_val),
                       "is_test": str(random.random() < TEST_TRAFFIC_RATE).lower()}
                rows.append(row)
                if random.random() < DUPLICATE_RATE:          # vendor redelivery / correction
                    corrected = row.copy()
                    corrected["revenue_usd"] = _messy_revenue(round(max(0.0, revenue_val + random.uniform(-1,3)), 2))
                    corrected["ingested_at"] = row["ingested_at"] if random.random() < DUPLICATE_TIE_RATE \
                        else (ingested_dt + timedelta(minutes=random.randint(1,2000))).strftime("%Y-%m-%d %H:%M:%S")
                    rows.append(corrected)
            d += timedelta(days=1)
    return pd.DataFrame(rows).sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def write_events_csv(events: pd.DataFrame, path: Path) -> None:
    """
    Writes events_raw.csv by hand (not df.to_csv) so we can seed rows with
    the WRONG number of fields -- something a well-formed DataFrame can
    never produce, but that vendor feeds do in real life (truncated
    delivery, a stray delimiter inside an unescaped field, etc.).
    """
    cols = list(events.columns)
    n = len(cols)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(cols)
        for row in events.itertuples(index=False, name=None):
            values = list(row)
            r = random.random()
            if r < STRUCTURAL_MALFORMED_RATE / 2:
                # too few fields: a field got dropped somewhere in the pipe
                drop_at = random.randint(0, n - 1)
                values = values[:drop_at] + values[drop_at + 1:]
            elif r < STRUCTURAL_MALFORMED_RATE:
                # too many fields: a stray extra value got appended/injected
                insert_at = random.randint(0, n)
                stray = random.choice(["extra", "", "0", "unexpected"])
                values = values[:insert_at] + [stray] + values[insert_at:]
            writer.writerow(values)


def gen_staging_events(apps: pd.DataFrame, costs: pd.DataFrame, events: pd.DataFrame) -> pd.DataFrame:
    """
    A second, later delivery batch for testing Task 2.4 (incremental load,
    safe to re-run): a mix of

      a) brand-new events, one day after the main extract's date range, and
      b) late corrections to event_ids that are ALREADY in events_raw.csv
         (same event_id, newer ingested_at, revised revenue) -- exactly the
         case an incremental load must absorb without creating a duplicate
         and without losing the correction.
    """
    start, catalog = _range_start(), _campaign_catalog()
    costed_pairs = costs.groupby("app_id")[["media_source", "campaign"]] \
        .apply(lambda d: list(map(tuple, d.values))).to_dict()
    last_day = start + timedelta(days=NUM_DAYS - 1)
    new_day = last_day + timedelta(days=1)
    rows = []

    max_seq = events["event_id"].str.lstrip("e").astype(int).max()
    seq = max_seq

    # a) brand-new events
    for _ in range(STAGING_NEW_EVENTS):
        app = apps.sample(1).iloc[0]
        pairs = costed_pairs.get(app["app_id"], [])
        seq += 1
        media_source, campaign = random.choice(pairs) if pairs and random.random() > ORGANIC_SHARE else random.choice(catalog)
        event_dt = datetime(new_day.year, new_day.month, new_day.day,
                             random.randint(0, 23), random.randint(0, 59), random.randint(0, 59),
                             tzinfo=timezone.utc)
        ingested_dt = event_dt + timedelta(minutes=random.randint(0, 240))
        revenue_val = round(max(0.0, random.gauss(2.0, 3.0)), 2) if random.random() < 0.3 else 0.0
        rows.append({
            "event_id": f"e{seq:08d}", "user_id": f"u{random.randint(1,50000)}",
            "app_id": app["app_id"],
            "event_name": random.choice(["impression", "click", "install", "purchase"]),
            "event_time": event_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "ingested_at": ingested_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "country": _messy_country(random.choice(COUNTRIES)),
            "media_source": media_source, "campaign": campaign,
            "revenue_usd": _messy_revenue(revenue_val),
            "is_test": str(random.random() < TEST_TRAFFIC_RATE).lower(),
        })

    # b) late corrections to already-delivered events
    sample_existing = events.drop_duplicates("event_id").sample(
        n=min(STAGING_CORRECTIONS, events["event_id"].nunique()), random_state=SEED
    )
    for _, base in sample_existing.iterrows():
        try:
            base_revenue = float(str(base["revenue_usd"]).replace(",", "."))
        except ValueError:
            base_revenue = 0.0
        new_ingested = datetime.strptime(base["ingested_at"], "%Y-%m-%d %H:%M:%S") \
            + timedelta(days=random.randint(1, 3), minutes=random.randint(0, 500))
        rows.append({
            "event_id": base["event_id"], "user_id": base["user_id"], "app_id": base["app_id"],
            "event_name": base["event_name"], "event_time": base["event_time"],
            "ingested_at": new_ingested.strftime("%Y-%m-%d %H:%M:%S"),
            "country": base["country"], "media_source": base["media_source"], "campaign": base["campaign"],
            "revenue_usd": _messy_revenue(round(max(0.0, base_revenue + random.uniform(-1, 3)), 2)),
            "is_test": base["is_test"],
        })

    return pd.DataFrame(rows).sample(frac=1.0, random_state=SEED).reset_index(drop=True)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    apps = gen_apps()
    costs = gen_campaign_costs(apps)
    events = gen_events_raw(apps, costs)
    staging = gen_staging_events(apps, costs, events)

    apps.to_csv(OUTPUT_DIR/"apps.csv", index=False)
    costs.to_csv(OUTPUT_DIR/"campaign_costs.csv", index=False)
    write_events_csv(events, OUTPUT_DIR/"events_raw.csv")
    staging.to_csv(OUTPUT_DIR/"staging_events.csv", index=False)

    print(f"apps: {len(apps)}, costs: {len(costs):,}, "
          f"events: {len(events):,} ({events.event_id.nunique():,} distinct), "
          f"staging: {len(staging):,}")

if __name__ == "__main__":
    main()