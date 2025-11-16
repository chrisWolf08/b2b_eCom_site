CREATE OR REPLACE TABLE neogen-ga4-export.reporting_tables.arpu_arppu_siteSearch
AS
-- Step 1: Scan the events table only ONCE to gather all necessary data points
WITH daily_events_summary AS (
  SELECT
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS event_date,
    user_pseudo_id,
    -- Extract values using conditional logic within the single scan
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
    IF(event_name = 'purchase', (SELECT value.double_value FROM UNNEST(event_params) WHERE key = 'value'), 0) AS revenue,
    IF(event_name IN ('user_engagement', 'session_start'), 1, 0) AS is_active_event,
    IF(event_name = 'view_search_results', 1, 0) AS is_search_event
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  WHERE
    _TABLE_SUFFIX BETWEEN '20250301' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
),

-- Step 2: Aggregate the daily event data to the user level
daily_user_aggregates AS (
  SELECT
    event_date,
    user_pseudo_id,
    -- Use MAX to get a single market_id per user per day.
    -- COALESCE handles cases where market_id might be NULL for some events.
    COALESCE(MAX(market_id), 'N/A') as market_id,
    -- Aggregate flags and revenue
    SUM(revenue) AS total_revenue,
    MAX(is_active_event) = 1 AS is_active_user,
    MAX(is_search_event) = 1 AS did_site_search
  FROM daily_events_summary
  GROUP BY event_date, user_pseudo_id
)

-- Step 3: Perform the final aggregation for the report
SELECT
  event_date,
  market_id,
  IF(did_site_search, 'Did Site Search', 'Did Not Site Search') AS site_search_segment,
  COUNT(DISTINCT user_pseudo_id) AS total_users,
  COUNT(DISTINCT IF(is_active_user, user_pseudo_id, NULL)) AS active_users,
  COUNT(DISTINCT IF(total_revenue > 0, user_pseudo_id, NULL)) AS paying_users,
  SUM(total_revenue) AS total_revenue,
  SAFE_DIVIDE(SUM(total_revenue), COUNT(DISTINCT IF(is_active_user, user_pseudo_id, NULL))) AS ARPU,
  SAFE_DIVIDE(SUM(total_revenue), COUNT(DISTINCT IF(total_revenue > 0, user_pseudo_id, NULL))) AS ARPPU
FROM daily_user_aggregates
GROUP BY event_date, market_id, site_search_segment;