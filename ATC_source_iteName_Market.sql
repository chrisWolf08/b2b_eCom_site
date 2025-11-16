WITH item_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', ev.event_date) AS date,
    (SELECT value.string_value FROM UNNEST(ev.event_params) WHERE key = 'market_id') AS market_id,
    ev.user_pseudo_id,
    CAST((SELECT value.int_value FROM UNNEST(ev.event_params) WHERE key = 'ga_session_id') AS STRING) AS ga_session_id,
    CONCAT(ev.user_pseudo_id, ':', CAST((SELECT value.int_value FROM UNNEST(ev.event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    ev.event_name,
    it.item_name      AS item_name,
    it.item_category  AS item_category,
    -- Extract add_to_cart_type from item parameters (if present)
    (SELECT param.value.string_value 
     FROM UNNEST(it.item_params) AS param 
     WHERE param.key = 'add_to_cart_type') AS add_to_cart_type
  FROM `neogen-ga4-export.analytics_331328809.events_*` AS ev
  CROSS JOIN UNNEST(ev.items) AS it
  WHERE
    _TABLE_SUFFIX BETWEEN '20250809'
      AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND ev.event_name IN ('add_to_cart', 'view_item')
    AND (SELECT value.int_value FROM UNNEST(ev.event_params) WHERE key = 'ga_session_id') IS NOT NULL
)

SELECT
  date,
  market_id,
  item_name,
  item_category,
  add_to_cart_type,
  COUNT(DISTINCT IF(event_name = 'add_to_cart', session_id, NULL)) AS sessions_add_to_cart,
  COUNT(DISTINCT IF(event_name = 'view_item',   session_id, NULL)) AS sessions_view_item
FROM item_events
GROUP BY date, market_id, item_name, item_category, add_to_cart_type
ORDER BY date DESC, sessions_add_to_cart DESC;