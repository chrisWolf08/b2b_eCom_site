CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.eCom_checkout_Error` 
AS
WITH SessionFlagsByDate AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
    -- Flag for starting checkout
    MAX(IF(event_name = 'begin_checkout', 1, 0)) AS has_checkout_begun,
    -- Flag for the error
    MAX(IF(EXISTS (
      SELECT 1 FROM UNNEST(event_params)
      WHERE key = 'error_id' AND value.string_value = 'checkout'
    ), 1, 0)) AS has_error,
    -- NEW: Flag for adding payment info
    MAX(IF(event_name = 'add_payment_info', 1, 0)) AS has_payment_info_added,
    -- NEW: Flag for making a purchase
    MAX(IF(event_name = 'purchase', 1, 0)) AS has_purchased
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  WHERE _TABLE_SUFFIX BETWEEN '20250915' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  GROUP BY 1, 2, 3
)
SELECT
  market_id,
  date,
  -- Total sessions that started the checkout process
  COUNT(DISTINCT IF(has_checkout_begun = 1, session_id, NULL)) AS total_checkout_sessions,
  -- Of those, how many had the error
  COUNT(DISTINCT IF(has_checkout_begun = 1 AND has_error = 1, session_id, NULL)) AS sessions_with_error,
  -- NEW: Of those, how many also added payment info
  COUNT(DISTINCT IF(has_checkout_begun = 1 AND has_payment_info_added = 1, session_id, NULL)) AS sessions_with_payment_info,
  -- NEW: Of those, how many also purchased
  COUNT(DISTINCT IF(has_checkout_begun = 1 AND has_purchased = 1, session_id, NULL)) AS sessions_with_purchase
FROM SessionFlagsByDate
GROUP BY date, market_id
ORDER BY date ASC;
