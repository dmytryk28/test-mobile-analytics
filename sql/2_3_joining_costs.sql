-- BigQuery
CREATE OR REPLACE TABLE raw.daily_revenue_and_cost AS
WITH daily_revenue AS (
    SELECT
        app_id,
        media_source,
        campaign,
        CAST(event_time AS DATE) AS activity_date,
        SUM(revenue_usd) AS revenue_usd
    FROM raw.clean_events
    GROUP BY app_id, media_source, campaign, CAST(event_time AS DATE)
),
daily_cost AS (
    SELECT
        app_id,
        media_source,
        campaign,
        date AS activity_date,
        SUM(COALESCE(SAFE_CAST(cost_usd AS NUMERIC), CAST(0.0 AS NUMERIC))) AS cost_usd,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks
    FROM raw.campaign_costs
    GROUP BY app_id, media_source, campaign, date
)
SELECT
    COALESCE(r.app_id, c.app_id) AS app_id,
    COALESCE(r.media_source, c.media_source) AS media_source,
    COALESCE(r.campaign, c.campaign) AS campaign,
    COALESCE(r.activity_date, c.activity_date) AS activity_date,
    COALESCE(r.revenue_usd, CAST(0.0 AS NUMERIC)) AS revenue_usd,
    COALESCE(c.cost_usd, CAST(0.0 AS NUMERIC)) AS cost_usd,
    c.impressions,
    c.clicks,
    CASE
        WHEN c.cost_usd IS NULL OR c.cost_usd = 0 THEN NULL
        ELSE ROUND(COALESCE(r.revenue_usd, CAST(0.0 AS NUMERIC)) / c.cost_usd, 2)
    END AS roas
FROM daily_revenue r
FULL OUTER JOIN daily_cost c
    ON r.app_id = c.app_id
    AND r.media_source = c.media_source
    AND r.campaign = c.campaign
    AND r.activity_date = c.activity_date;