CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.purchase_funnel_userlevel_byCountry` AS
WITH
  -- 1. Get all relevant events and extract market_id
  daily_events AS (
    SELECT
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS date,
      user_pseudo_id,
      event_name,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id
    FROM
      `neogen-ga4-export.analytics_331328809.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20250401' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
  ),

  -- 2. Calculate the total unique users per day and market
  total_users AS (
    SELECT
      date,
      market_id,
      COUNT(DISTINCT user_pseudo_id) AS total_users
    FROM
      daily_events
    GROUP BY
      date,
      market_id
  ),

  -- 3. Filter down to only the specific funnel step events
  filtered_events AS (
    SELECT
      *
    FROM
      daily_events
    WHERE
      event_name IN (
        'view_item_list',
        'view_item',
        'add_to_cart',
        'purchase'
      )
  ),

  -- 4. Pivot the data to get a distinct user count for each funnel stage by market
  stage_users AS (
    SELECT
      date,
      market_id,
      COUNT(DISTINCT IF(event_name = 'view_item_list', user_pseudo_id, NULL)) AS view_item_list_users,
      COUNT(DISTINCT IF(event_name = 'view_item', user_pseudo_id, NULL)) AS view_item_users,
      COUNT(DISTINCT IF(event_name = 'add_to_cart', user_pseudo_id, NULL)) AS add_to_cart_users,
      COUNT(DISTINCT IF(event_name = 'purchase', user_pseudo_id, NULL)) AS purchase_users
    FROM
      filtered_events
    GROUP BY
      date,
      market_id
  )

-- 5. Join the stage counts with total user counts and calculate rates
SELECT
  s.date,
  s.market_id,
  t.total_users,
  s.view_item_list_users,
  s.view_item_users,
  s.add_to_cart_users,
  s.purchase_users,

FROM
  stage_users AS s
  JOIN total_users AS t USING (date, market_id);