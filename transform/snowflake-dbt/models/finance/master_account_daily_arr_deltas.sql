{{config({
    "materialized": "table",
    "schema": "finance"
  })
}}

WITH master_account_daily_arr_deltas AS (
    SELECT
        master_account_util_dates.day::DATE AS new_day,
        (master_account_util_dates.day - interval '1 day')::DATE AS previous_day,
        master_account_util_dates.master_account_sfid AS master_account_sfid,
        coalesce(new_day.total_arr_norm,0) AS new_day_total_arr_norm,
        CASE
            WHEN master_account_util_dates.day = (SELECT min(day) FROM {{ ref('master_account_daily_arr') }} AS original WHERE new_day.master_account_sfid = original.accounmaster_account_sfidt_sfid ) 
                THEN 0 
            ELSE  coalesce(previous_day.total_arr_norm,0)
        END  AS previous_day_total_arr_norm,
        CASE
            WHEN master_account_util_dates.day = (SELECT min(day) FROM {{ ref('master_account_daily_arr') }} AS original WHERE new_day.master_account_sfid = original.master_account_sfid ) 
                THEN true 
            ELSE  false 
        END  AS master_account_new_arr,
        coalesce(new_day.total_arr_norm,0) -
        CASE
            WHEN master_account_util_dates.day = (SELECT min(day) FROM {{ ref('master_account_daily_arr') }} AS original WHERE new_day.master_account_sfid = original.master_account_sfid ) 
                THEN 0 
            ELSE  coalesce(previous_day.total_arr_norm,0)
        END  AS total_arr_norm_delta,
        coalesce(new_day.total_arr,0) AS new_day_total_arr,
        CASE
            WHEN master_account_util_dates.day = (SELECT min(day) FROM {{ ref('master_account_daily_arr') }} AS original WHERE new_day.master_account_sfid = original.master_account_sfid ) 
                THEN 0 
            ELSE  coalesce(previous_day.total_arr,0) 
        END  AS previous_day_total_arr,
        coalesce(new_day.total_arr,0) -
        CASE
            WHEN master_account_util_dates.day = (SELECT min(day) FROM {{ ref('master_account_daily_arr') }} AS original WHERE new_day.master_account_sfid = original.master_account_sfid ) 
                THEN 0 
            ELSE  coalesce(previous_day.total_arr,0)
        END  AS total_arr_delta
    FROM {{ ref('master_account_util_dates') }}
    LEFT JOIN {{ ref('master_account_daily_arr') }} AS new_day  ON  master_account_util_dates.master_account_sfid = new_day.master_account_sfid AND master_account_util_dates.day = new_day.day
    LEFT JOIN {{ ref('master_account_daily_arr') }} AS previous_day  ON  master_account_util_dates.master_account_sfid = previous_day.master_account_sfid AND master_account_util_dates.day - interval '1 day' = previous_day.day
)
SELECT * FROM master_account_daily_arr_deltas