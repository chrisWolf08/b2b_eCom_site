
WITH raw_events AS (
  SELECT
    PARSE_DATE('%Y%m%d', _TABLE_SUFFIX) AS event_date,
    user_pseudo_id,
    -- Extract Revenue (treat nulls as 0)
    COALESCE(ecommerce.purchase_revenue, 0) AS purchase_value,
    -- Extract Registration Step Value
    CASE 
      WHEN event_name = 'registration_step' THEN (
        SELECT value.string_value 
        FROM UNNEST(event_params) 
        WHERE key = 'registration_step'
      )
      ELSE NULL 
    END AS step_value_raw
  FROM
    `neogen-ga4-export.analytics_331328809.events_*`
  WHERE
    -- Dynamic Date Filter: From March 1st to Yesterday
    _TABLE_SUFFIX BETWEEN '20251120' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
    AND event_name IN ('registration_step', 'purchase')
),

cleaned_events AS (
  SELECT
    event_date,
    user_pseudo_id,
    purchase_value,
    LOWER(TRIM(step_value_raw)) AS step_value
  FROM
    raw_events
  WHERE
    user_pseudo_id IS NOT NULL
),

user_daily_flags AS (
  SELECT
    event_date,
    user_pseudo_id,
    SUM(purchase_value) AS user_daily_revenue,
    
    -- Funnel Step 1: Start
    LOGICAL_OR(step_value = 'pageview') AS did_start_form,
    
    -- Funnel Step 2: Complete
    LOGICAL_OR(step_value IN ('registration complete', 'registration_complete')) AS did_finish_form,
    
    -- Funnel Step 3: Convert (Purchase > 0)
    LOGICAL_OR(purchase_value > 0) AS did_purchase

  FROM
    cleaned_events
  GROUP BY
    1, 2
)

SELECT
  event_date,
  
  -- 1. Starters
  COUNTIF(did_start_form) AS users_started,
  ROUND(SUM(CASE WHEN did_start_form THEN user_daily_revenue ELSE 0 END), 2) AS revenue_pool_starters,

  -- 2. Completers (The Form Success)
  COUNTIF(did_finish_form) AS users_registered,
  
  -- 3. Converters (The Business Success)
  COUNTIF(did_finish_form AND did_purchase) AS users_converted_same_day,

  -- 4. Realized Revenue (Strict Attribution)
  ROUND(SUM(CASE WHEN did_finish_form THEN user_daily_revenue ELSE 0 END), 2) AS revenue_from_registrations

FROM
  user_daily_flags
GROUP BY
  1
ORDER BY
  1 DESC;