CREATE OR REPLACE TABLE `neogen-ga4-export.reporting_tables.eCom_User_errorRecovery_Rate_onlyCheckout` 
AS
WITH
  UserErrorEvents AS (
    SELECT
      user_pseudo_id,
      event_timestamp,
      PARSE_DATE('%Y%m%d', event_date) as event_date,
      (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'market_id') AS market_id,
      ROW_NUMBER() OVER(PARTITION BY user_pseudo_id ORDER BY event_timestamp ASC) as error_rank
    FROM
      `neogen-ga4-export.analytics_331328809.events_*`
    WHERE
      _TABLE_SUFFIX BETWEEN '20250501' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND EXISTS (
        SELECT 1 FROM UNNEST(event_params)
        WHERE key = 'error_id' AND value.string_value = 'checkout'
      )
  ),
  UsersWithError AS (
    -- Step 1: Filter for only the very first error for each user to get their cohort date and market.
    SELECT
      user_pseudo_id,
      market_id,
      event_date AS first_error_date,
      event_timestamp AS first_error_timestamp
    FROM UserErrorEvents
    WHERE error_rank = 1
  ),
  RecoveredUsers AS (
    -- Step 2: From the group of users with errors, find out which ones made a purchase AFTER their first error.
    SELECT DISTINCT
      uwe.user_pseudo_id
    FROM
      UsersWithError uwe
      -- Join back to the main events table to look for purchases
      JOIN `neogen-ga4-export.analytics_331328809.events_*` AS events
        ON uwe.user_pseudo_id = events.user_pseudo_id
    WHERE
      events._TABLE_SUFFIX BETWEEN '20250501' AND FORMAT_DATE('%Y%m%d', DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY))
      AND events.event_name = 'purchase'
      -- This is the crucial condition: the purchase must happen AFTER the error.
      AND events.event_timestamp > uwe.first_error_timestamp
  )
-- Step 3: Aggregate the results by the date of the first error and market, then calculate the recovery rate.
SELECT
  uwe.first_error_date,
  uwe.market_id,
  COUNT(DISTINCT uwe.user_pseudo_id) AS total_users_with_error,
  COUNT(DISTINCT ru.user_pseudo_id) AS recovered_users
  
FROM
  UsersWithError uwe
  LEFT JOIN RecoveredUsers ru ON uwe.user_pseudo_id = ru.user_pseudo_id
GROUP BY
  1, 2
ORDER BY
  first_error_date ASC, market_id;
