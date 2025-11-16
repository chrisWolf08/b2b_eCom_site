
WITH item_events AS (
  SELECT
    event_name,
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(ev.event_params) WHERE key = 'market_id') AS market_id,
    CONCAT(ev.user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(ev.event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    it.item_name      AS item_name,
    it.item_category  AS item_category,
    SAFE_CAST(it.quantity AS INT64)          AS qty,
    SAFE_CAST(it.price    AS NUMERIC)        AS price,
    -- Extract add_to_cart_type from item parameters
    (SELECT param.value.string_value 
     FROM UNNEST(it.item_params) AS param 
     WHERE param.key = 'add_to_cart_type') AS add_to_cart_type
  FROM  `neogen-ga4-export.analytics_331328809.events_*`  AS ev
  CROSS JOIN UNNEST(ev.items) AS it                      -- explode to item rows
  WHERE
        -- Reporting window: 1 Jan 2025 → yesterday
        _TABLE_SUFFIX BETWEEN '20250501'
                          AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
)

SELECT
  date,
  session_id,
  add_to_cart_type,
  item_name,
  item_category
FROM item_events
  WHERE item_name IS NOT NULL
  AND item_category IS NOT NULL
  AND add_to_cart_type IS NOT NULL
ORDER BY date DESC;