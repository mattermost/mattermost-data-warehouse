{{config({
    "materialized": "table",
    "schema": "bizops"
  })
}}

WITH contract_rates AS (

    SELECT *
    FROM {{ source('bizops','snowflake_contract_rates')}}

), contract_rate_rework AS (

    SELECT  effective_date AS effective_start_date,
            dateadd(day, -1, lead(effective_date, 1, '2059-01-01') OVER (ORDER BY effective_date ASC)) AS effective_end_date,
            rate
    FROM contract_rates

), date_spine AS (

  {{ dbt_utils.date_spine(
      start_date="to_date('11/01/2009', 'mm/dd/yyyy')",
      datepart="day",
      end_date="dateadd(year, 40, current_date)"
     )
  }}

), date_details AS (

    SELECT  date_day,
            DATE_PART(month, date_day) AS month_actual,
            DATE_PART(year, date_day) AS year_actual,
            FIRST_VALUE(date_day) OVER (PARTITION BY year_actual, month_actual ORDER BY date_day) AS first_day_of_month
    FROM date_spine

), rate_amortized AS (

    SELECT contract_rate_rework.*,
           date_details.date_day
    FROM contract_rate_rework
    LEFT JOIN date_details ON date_details.date_day >= contract_rate_rework.effective_start_date
      AND date_details.date_day <= contract_rate_rework.effective_end_date

)

SELECT *
FROM rate_amortized
