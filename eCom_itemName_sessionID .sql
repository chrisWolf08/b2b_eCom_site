SELECT
    PARSE_DATE('%Y%m%d', event_date) AS date,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
    CONCAT(user_pseudo_id, CAST((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS STRING)) AS session_id,
    item.item_name,
    item.item_category
  FROM `neogen-ga4-export.analytics_331328809.events_*`
  LEFT JOIN UNNEST(items) AS item
  WHERE event_name = 'view_item'
    AND _TABLE_SUFFIX BETWEEN '20250501'
        AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
