-- BigQuery
CREATE OR REPLACE TABLE raw.clean_events AS
WITH parsed AS (
    SELECT
        event_id, user_id, app_id, event_name,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', event_time) AS event_time,
        SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ingested_at) AS ingested_at,
        country, media_source, campaign,
        COALESCE(
            SAFE_CAST(REPLACE(NULLIF(TRIM(revenue_usd), ''), ',', '.') AS NUMERIC), 0.0
        ) AS revenue_usd,
        COALESCE(SAFE_CAST(is_test AS BOOL), FALSE) AS is_test
    FROM raw.events_raw
),
ranked_events AS (
    SELECT * EXCEPT(is_test)
    FROM parsed
    WHERE is_test = FALSE
        AND event_time IS NOT NULL
        AND ingested_at IS NOT NULL
        AND NULLIF(TRIM(event_id), '') IS NOT NULL
        AND NULLIF(TRIM(app_id), '') IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY event_id
        ORDER BY
            ingested_at DESC,
            FARM_FINGERPRINT(
                COALESCE(event_id,'') || '-' || COALESCE(user_id,'') || '-' ||
                COALESCE(app_id,'') || '-' || COALESCE(event_name,'') || '-' ||
                COALESCE(CAST(event_time AS STRING),'')  || '-' ||
                COALESCE(CAST(ingested_at AS STRING),'') || '-' ||
                COALESCE(country,'') || '-' || COALESCE(media_source,'') || '-' ||
                COALESCE(campaign,'') || '-' || COALESCE(CAST(revenue_usd AS STRING),'')
            ) DESC
    ) = 1
)
SELECT *
FROM ranked_events;