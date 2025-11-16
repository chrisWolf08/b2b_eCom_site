-- One row per session with flags for each add_to_cart_type

CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.eCom_itemName_atcSource_sessions_filtered` AS


WITH atc_item AS (
  SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
    it.item_category AS item_category,
    it.item_name     AS item_name,
    CONCAT(
      user_pseudo_id,
      CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)
    ) AS session_id,
    LOWER((SELECT p.value.string_value FROM UNNEST(it.item_params) p WHERE p.key = 'add_to_cart_type')) AS add_to_cart_type
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  CROSS JOIN UNNEST(items) AS it
  WHERE
    _TABLE_SUFFIX BETWEEN '20250805' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name = 'add_to_cart'
),
atc_clean AS (
  SELECT date, market_id, session_id, item_category, item_name, add_to_cart_type
  FROM atc_item
  WHERE add_to_cart_type IN (
      'pdp','quick_order','my_account_orders','search','my_account_lists',
      'pcp','pdp_related','cms_product_carousel','product_recommendations'
    )
),
session_item_atc_once AS (
  -- one row per (session, item, type)
  SELECT DISTINCT date, market_id, session_id, item_category, item_name, add_to_cart_type
  FROM atc_clean
)
SELECT
  date,
  market_id,
  item_category,
  item_name,
  add_to_cart_type,
  COUNT(*) AS sessions_add_to_cart
FROM session_item_atc_once
GROUP BY date, market_id, item_category, item_name, add_to_cart_type
ORDER BY date DESC, market_id, add_to_cart_type;
