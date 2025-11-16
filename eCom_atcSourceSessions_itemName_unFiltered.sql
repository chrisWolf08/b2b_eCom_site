
CREATE OR REPLACE VIEW `neogen-ga4-export.reporting_tables.eCom_atcSourceSessions_unfiltered_itemName` 
AS

WITH A_src AS (
  SELECT
    CAST(date AS DATE)                            AS date,
    UPPER(TRIM(market_id))                        AS market_id,
    UPPER(TRIM(item_category))                    AS item_category,
    UPPER(TRIM(item_name))                        AS item_name,
    LOWER(TRIM(add_to_cart_type))                 AS add_to_cart_type,
    SUM(COALESCE(sessions_add_to_cart, 0))        AS sessions_add_to_cart
  FROM `neogen-ga4-export.reporting_tables.eCom_itemName_atcSource_sessions_filtered`
  GROUP BY 1,2,3,4,5
),

-- B) Normalize + aggregate unfiltered item metrics
B_base AS (
  SELECT
    CAST(date AS DATE)                            AS date,
    UPPER(TRIM(market_id))                        AS market_id,
    UPPER(TRIM(item_category))                    AS item_category,
    UPPER(TRIM(item_name))                        AS item_name,
    SUM(COALESCE(view_item_sessions, 0))          AS view_item_sessions,
    SUM(COALESCE(sessions_purchase, 0))           AS sessions_purchase,
    SUM(COALESCE(item_revenue, 0))                AS item_revenue
    -- do NOT sum sessions_add_to_cart here; we want A_src filtered by source
  FROM `neogen-ga4-export.reporting_tables.eCom_item_sessions_market`
  GROUP BY 1,2,3,4
)

SELECT
  b.date,
  b.market_id,
  a.add_to_cart_type,                 -- dimension from A (source)
  b.item_category,
  b.item_name,
  b.view_item_sessions,               -- unfiltered
  IFNULL(a.sessions_add_to_cart, 0)   AS sessions_add_to_cart,  -- filtered by source
  b.sessions_purchase,                -- unfiltered
  b.item_revenue                      -- unfiltered
FROM B_base b
LEFT JOIN A_src a
  ON a.date          = b.date
 AND a.market_id     = b.market_id
 AND a.item_category = b.item_category
 AND a.item_name     = b.item_name
ORDER BY b.date DESC, b.market_id, a.add_to_cart_type, b.item_category, b.item_name;