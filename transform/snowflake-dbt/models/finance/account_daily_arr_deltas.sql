{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":["nightly"]
  })
}}

WITH account_daily_arr_deltas AS (
    SELECT
        account_util_dates.day::DATE AS new_day,
        (account_util_dates.day - interval '1 day')::DATE AS previous_day,
        account_util_dates.master_account_sfid AS master_account_sfid,
        account_util_dates.account_sfid AS account_sfid,
        CASE
            WHEN account_util_dates.day = (SELECT min(day) FROM finance.account_daily_arr AS original WHERE new_day.account_sfid = original.account_sfid ) 
                THEN true 
            ELSE  false 
        END  AS account_new_arr,
        CASE
            WHEN DATE_PART('day' ,account_util_dates.day) = 1
                THEN true
            ELSE false
        END AS month_start,
        CASE
            WHEN DATE_PART('day', account_util_dates.day + interval '1 day') = 1
                THEN true
            ELSE false 
        END AS month_end,
        coalesce(new_day.total_arr,0) AS new_day_total_arr,
        CASE
            WHEN account_util_dates.day = (SELECT min(day) FROM finance.account_daily_arr AS original WHERE new_day.account_sfid = original.account_sfid ) 
                THEN 0 
            ELSE  coalesce(previous_day.total_arr,0) 
        END  AS previous_day_total_arr,
        coalesce(new_day.total_arr,0) -
        CASE
            WHEN account_util_dates.day = (SELECT min(day) FROM finance.account_daily_arr AS original WHERE new_day.account_sfid = original.account_sfid ) 
                THEN 0 
            ELSE  coalesce(previous_day.total_arr,0)
        END  AS total_arr_delta
    FROM {{ ref('account_util_dates') }}
    LEFT JOIN {{ ref('account_daily_arr') }} AS new_day  ON  account_util_dates.account_sfid = new_day.account_sfid AND account_util_dates.day = new_day.day
    LEFT JOIN {{ ref('account_daily_arr') }} AS previous_day  ON  account_util_dates.account_sfid = previous_day.account_sfid AND account_util_dates.day - interval '1 day' = previous_day.day
)
SELECT * FROM account_daily_arr_deltas