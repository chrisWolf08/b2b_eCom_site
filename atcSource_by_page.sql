CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.atc_source_sessions_by_page` AS
WITH
-- 1) Base events with sessioning, URLs, market, and host
base_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    user_pseudo_id,
    CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
    event_timestamp,
    event_name,

    -- Dimensions
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
    LOWER(TRIM((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'))) AS page_location_raw,
    LOWER(TRIM(device.web_info.hostname)) AS hostname
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20250805'
      AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name IN ('page_view','view_item','add_to_cart')
),

-- 3) Final attribution label + attributed page location
final_attribution AS (
  SELECT
    date,
    market_id,
    hostname,
    user_pseudo_id,
    ga_session_id,
    event_timestamp AS atc_timestamp,
    page_location_raw AS atc_page_location,
    page_location_raw AS attributed_page_location
  FROM base_events
  WHERE event_name = 'add_to_cart'
    AND ga_session_id IS NOT NULL
),

pageviews_by_location AS (
  SELECT
    date,
    hostname,
    page_location_raw AS page_location,
    COUNT(DISTINCT CONCAT(user_pseudo_id, ga_session_id)) AS sessions_on_page
  FROM base_events
  WHERE event_name IN ('page_view', 'view_item')
    AND ga_session_id IS NOT NULL
  GROUP BY date, hostname, page_location
)

-- 4) Output (requested fields)
SELECT
  fa.date,
  fa.market_id,
  fa.hostname,
  ANY_VALUE(pv.sessions_on_page) AS sessions_on_page
  COUNT(DISTINCT CONCAT(fa.user_pseudo_id, fa.ga_session_id)) AS sessions_with_add_to_cart,
  fa.attributed_page_location                               AS page_location
FROM final_attribution fa
LEFT JOIN pageviews_by_location pv
  ON fa.date = pv.date
  AND fa.hostname = pv.hostname
  AND fa.attributed_page_location = pv.page_location
GROUP BY fa.date, fa.market_id, fa.hostname, page_location
ORDER BY fa.date DESC, fa.market_id, fa.hostname;