# 2. SQL

## 2.1 De-duplication

If two deliveries of the same event_id have the same ingested_at,
sorting by ingested_at alone does not determine their relative order -
ROW_NUMBER() can pick a different row on different runs. To fix this, a
second sort key was added: FARM_FINGERPRINT() over all row fields.
It returns the same hash for the same input, so the choice between
the two rows is fixed and repeatable.
ingested_at DESC itself picks which delivery wins (the newer one) -
getting that direction wrong keeps stale revenue instead of the
correction. FARM_FINGERPRINT only guarantees determinism for the tie and
not the "correct" pick.

## 2.2 Window functions

Days with zero events are missing from clean_events - there is no row with revenue = 0.
This matters for the 7-day moving average: if you average only existing rows, a "7-day"
window can stretch over more than 7 calendar days when days get skipped.
The fix is to build a calendar of every day per app, from launch date to the last date with data,
and left join actual revenue onto it, filling missing days with zero.
Averaging over that filled table with a row-based window guarantees 7 calendar rows per
window even when there are gaps.

## 2.3 Joining costs

Used FULL OUTER JOIN on app_id + media_source + campaign + activity_date,
because both mismatch cases matter:

1. Cost with no revenue: money was spent but drove no measurable events.
The campaign underperformed, or there's a tracking/attribution gap.

2. Revenue with no cost: usually organic traffic with no paid campaign behind it,
or the vendor hasn't delivered that day's data yet.

Division by zero: ROAS is set to NULL when cost_usd is NULL or 0.
NULL means "undefined," which is different from 0. Treating them the same would make no-cost
rows look like failed campaigns.

## 2.4 Incremental load

Filtered on ingested_at, not event_time. ingested_at is when a row became available to load.
event_time is when the event happened on the device - a late row can have
an old event_time but a new ingested_at.

The load uses MERGE on event_id: update only if src.ingested_at is newer than the target's,
insert if there's no match. On the second run the comparison is false for rows already applied,
so nothing changes.

The smallest safe reload window is bounded by how late data can arrive. If a source can deliver
event up to N days after it happened, the incremental job has to look back at least N days of
ingested_at each run. The cost is reprocessing the same rows, and the wider the window,
the more compute per run.

# 3. Python

## 3.2. A small pipeline

If the script crashes mid-write, some date partitions get overwritten with new data
and others don't. The output ends up a mix of old and new. The current script doesn't protect
against this. Possible fix: write everything to a temp folder, then move it into place
after all partitions finish successfully - so a crash never leaves half-updated output.

## 3.3. Reading someone else's code

Problems:

1. No dedup, no is_test filter. Worst bug because it gives wrong totals without crushing. 
2. astype(float) fails on "NULL", blanks, comma-decimals. 
3. str.startswith(day) breaks on malformed timestamps, ignores timezone. 
4. iterrows() is slow - loop instead of vectorized aggregation.

Rewrite takes an already-cleaned DataFrame instead of a raw path, and uses groupby().sum().
Fixes correctness and speed together - dedup/test-filter/valid revenue are already handled upstream,
so this function only aggregates.

## 4.1. Star schema

### fact_events
Grain: one row per deduplicated, non-test event_id.

Fields: event_id, event_date (FK to dim_date), app_id (FK to dim_app),
campaign_key (FK to dim_campaign), country (FK to dim_geo), user_id
(degenerate dimension, no separate dim_user), revenue_usd (measure).

Measures: revenue_usd, plus derived ones from 2.2 (daily revenue, running total, moving average).

### fact_campaign_costs
Costs don't share fact_events's grain, so they're a separate fact table.

Grain: one row per date, app_id, media_source, campaign.

Fields: event_date (FK to dim_date), app_id (FK to dim_app), campaign_key (FK to dim_campaign),
cost_usd, impressions, clicks (measures).

### Dimensions
- dim_date: date, day_of_week, month, year
- dim_app: app_id (PK), app_name, platform, store_id, launched_on
- dim_geo: country_code (PK), country_name
- dim_campaign: campaign_key (surrogate PK), media_source, campaign, valid_from, valid_to, is_current (SCD Type 2, see 4.2)

## 4.2. A dimension that changes

Type 1 (overwrite) is out, it would rewrite yesterday's published numbers under the new name.
Use Type 2 on dim_campaign: surrogate key, valid_from, valid_to, is_current.
Facts reference the surrogate key, not the display name, so old rows keep the old name and new rows
get the new one. Cost: querying a campaign's full history under one label needs an extra join through
a stable natural key, a plain group by campaign name is not enough.

## 4.3 Idempotency

To a marketing manager:

Think of idempotency like re-clicking "submit payment". 
It means you can run the pipeline twice by accident and nothing bad happens. 
The second run doesn't double rows or revenue numbers.

To an engineer:

A plain INSERT ... SELECT has no memory of what it already inserted,
run it twice for the same day, and you get two copies, doubling downstream sums.
Fixes: delete the target partition before inserting, a MERGE keyed on a unique key.


## 4.4. Late data

The incremental load filters on ingested_at, so a Thursday-arriving event with a Monday event_time
updates that row in Thursday's MERGE, changing an already-published Monday number.
What I'd do: flag when a load changes a closed day's total past some threshold,
so it's visible instead of silent drift. Tell the client the number was revised because mobile data can arrive late.
Maybe, agree on an SLA upfront, for example numbers are preliminary for N days and final after.

## 4.5. Partitioning

Partition fact_events on event_date, not ingested_at. The main read pattern,
dashboards and ROAS by day, asks what happened on day X, so event_date lets those queries
prune to one partition. Cost: late events force rewriting past partitions on every load,
and a query like what arrived in the last hour gets slower, scanning many partitions instead of one.

## 5. Debugging scenario

1. Does raw or staging data for that app have any Sunday rows at all?
YES - 2.
NO - vendor-side gap for that app.

2. Are all that app's Sunday rows flagged is_test true?
YES - found it, though a whole day mis-flagged is odd and worth checking why.
NO - 3.

3. Did dedup or a timezone boundary eat the rows, a wrong tie-break or event_time shifting
Sunday's events into a neighboring day's partition?
YES - found it.
NO - 4.

4. Did Sunday's window actually get processed, or did a per-app step fail silently while
the orchestrator reported overall success?
YES (covered correctly) - 5.
NO (silently skipped) - found it, success needs tracking per app or partition, not just job-level.

5. Does app_id join cleanly end to end, same casing, no silent rename between fact and dimension?
NO - found it.
YES (and still zero) - bisect row counts at every stage, raw to clean to daily aggregate to dashboard,
and check if the job ran before the UTC day fully closed.
