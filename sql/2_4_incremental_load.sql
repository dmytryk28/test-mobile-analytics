-- BigQuery
MERGE INTO raw.clean_events AS tgt
USING (
    WITH parsed AS (
        SELECT
            event_id, user_id, app_id, event_name,
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', event_time) AS event_time,
            SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', ingested_at) AS ingested_at,
            country, media_source, campaign,
            COALESCE(
                SAFE_CAST(REPLACE(NULLIF(TRIM(revenue_usd), ''), ',', '.') AS NUMERIC),
                CAST(0.0 AS NUMERIC)
            ) AS revenue_usd,
            COALESCE(SAFE_CAST(LOWER(TRIM(is_test)) AS BOOL), FALSE) AS is_test
        FROM raw.staging_events
    )
    SELECT event_id, user_id, app_id, event_name, event_time,
           ingested_at, country, media_source, campaign, revenue_usd
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
) AS src
ON tgt.event_id = src.event_id
WHEN MATCHED AND src.ingested_at > tgt.ingested_at THEN
    UPDATE SET
        user_id = src.user_id,
        app_id = src.app_id,
        event_name = src.event_name,
        event_time = src.event_time,
        ingested_at = src.ingested_at,
        country = src.country,
        media_source = src.media_source,
        campaign = src.campaign,
        revenue_usd = src.revenue_usd
WHEN NOT MATCHED THEN
    INSERT (event_id, user_id, app_id, event_name, event_time,
            ingested_at, country, media_source, campaign, revenue_usd)
    VALUES (src.event_id, src.user_id, src.app_id, src.event_name, src.event_time,
            src.ingested_at, src.country, src.media_source, src.campaign, src.revenue_usd);