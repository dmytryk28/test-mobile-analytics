import pandas as pd


def load_daily(clean_events: pd.DataFrame, day: str) -> pd.DataFrame:
    day_start = pd.Timestamp(day, tz="UTC")
    day_end = day_start + pd.Timedelta(days=1)
    daily = clean_events[
        (clean_events["event_time"] >= day_start)
        & (clean_events["event_time"] < day_end)
    ]
    return (
        daily.groupby(["app_id", "media_source"], as_index=False)["revenue_usd"]
        .sum()
        .rename(columns={"revenue_usd": "revenue"})
        .assign(key=lambda d: d["app_id"].astype(str) + "-" + d["media_source"].astype(str))
        .loc[:, ["key", "revenue"]]
        .sort_values("revenue", ascending=False)
        .reset_index(drop=True)
    )