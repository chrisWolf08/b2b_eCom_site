-- Build a per‑session counter of PDP info-link clicks
WITH link_clicks_filtered AS (
  SELECT
    date,
    session_id,
    COUNTIF(link_classes = 'product-details__info-link scroll-to-link') AS info_link_clicks
  FROM `neogen-ga4-export.reporting_tables.eCom_itemName_interactions`
  GROUP BY date, session_id
),

-- Join session base with duration, clicks, and ATC source
base AS (
  SELECT
    s.date,
    s.market_id,
    s.session_id,
    s.item_name,
    s.item_category,
    d.duration_seconds,
    COALESCE(i.info_link_clicks, 0) AS info_link_clicks,
    a.add_to_cart_type
  FROM `neogen-ga4-export.reporting_tables.eCom_itemName_sessionID`           AS s
  LEFT JOIN `neogen-ga4-export.reporting_tables.eCom_itemName_durationSeconds` AS d
    ON s.session_id     = d.session_id
   AND s.item_name      = d.item_name
   AND s.item_category  = d.item_category
   AND s.date           = d.date
  LEFT JOIN link_clicks_filtered AS i
    ON s.session_id     = i.session_id
   AND s.date           = i.date
  LEFT JOIN (
    SELECT *
  `neogen-ga4-export.reporting_tables.eCom_itemName_atcType_SessionID` 
  WHERE add_to_cart_type IN (
    'PDP',
    'Search',
    'my_account_orders',
    'pcp',
    'my_account_lists',
    'quick_order',
    'pdp_related',
    'cms_product_carousel',
    'product_recommendations'
  )
) AS a  
    ON s.session_id     = a.session_id
   AND s.item_name      = a.item_name
   AND s.item_category  = a.item_category
   AND s.date           = a.date
)

SELECT
  date,
  market_id,
  item_name,
  item_category,
  add_to_cart_type,
  duration_seconds,
  info_link_clicks,
  CASE
    WHEN duration_seconds >= 10 AND info_link_clicks >= 2 THEN 1
    ELSE 0
  END AS passed_filters_flag
FROM base
ORDER BY date DESC, date;