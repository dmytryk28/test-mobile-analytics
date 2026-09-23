-- BigQuery
WITH daily_revenue AS (
    SELECT
        app_id,
        CAST(event_time AS DATE) AS event_date,
        SUM(revenue_usd) AS revenue
    FROM raw.clean_events
    GROUP BY app_id, CAST(event_time AS DATE)
),
calendar AS (
    SELECT a.app_id, event_date
    FROM raw.apps a,
    UNNEST(GENERATE_DATE_ARRAY(
        a.launched_on,
        (SELECT MAX(dr.event_date) FROM daily_revenue dr WHERE dr.app_id = a.app_id),
        INTERVAL 1 DAY
    )) AS event_date
    WHERE EXISTS (SELECT 1 FROM daily_revenue dr WHERE dr.app_id = a.app_id)
),
revenue_filled AS (
    SELECT
        c.app_id,
        c.event_date,
        COALESCE(dr.revenue, 0.0) AS revenue
    FROM calendar c
    LEFT JOIN daily_revenue dr
        ON dr.app_id = c.app_id AND dr.event_date = c.event_date
)
SELECT
    app_id,
    event_date,
    revenue AS daily_revenue,
    SUM(revenue) OVER (
        PARTITION BY app_id ORDER BY event_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total_revenue,
    ROUND(AVG(revenue) OVER (
        PARTITION BY app_id ORDER BY event_date
        ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS moving_avg_7d,
    CASE
        WHEN LAG(revenue) OVER (PARTITION BY app_id ORDER BY event_date) IS NULL
          OR LAG(revenue) OVER (PARTITION BY app_id ORDER BY event_date) = 0
        THEN NULL
        ELSE ROUND(
            100.0 * (revenue - LAG(revenue) OVER (PARTITION BY app_id ORDER BY event_date))
                / LAG(revenue) OVER (PARTITION BY app_id ORDER BY event_date),
            2
        )
    END AS dod_change
FROM revenue_filled
ORDER BY app_id, event_date;