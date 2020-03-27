{{config({
    "materialized": "table",
    "schema": "bizops"
  })
}}

WITH base AS (

	SELECT *
	FROM {{ source('account_usage', 'warehouse_metering_history') }}


), contract_rates AS (

    SELECT *
    FROM {{ ref('snowflake_amortized_rates') }}

), usage AS (

        SELECT warehouse_id,
               warehouse_name,
               start_time,
               end_time,
               date_trunc('month', end_time)::date          AS usage_month,
               date_trunc('day', end_time)::date            AS usage_day,
               datediff(hour, start_time, end_time)         AS usage_length,
               contract_rates.rate                          AS credit_rate,
               round(credits_used * contract_rates.rate, 2) AS dollars_spent
        FROM base
        LEFT JOIN contract_rates ON date_trunc('day', end_time) = contract_rates.date_day

)

SELECT *
FROM usage
