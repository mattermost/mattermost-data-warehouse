{{
  config({
    "materialized": 'table',
    "schema": "util"
  })
}}

WITH date_spine AS (

  {{ dbt_utils.date_spine(
      start_date="to_date('02/01/2010', 'mm/dd/yyyy')",
      datepart="day",
      end_date="dateadd(year, 5, current_date)"
     )
  }}

), calculated as (

    SELECT  
      date_day,
      date_day                                                                                AS date_actual,

      DAYNAME(date_day)                                                                       AS day_name,

      DATE_PART('month', date_day)                                                            AS month_actual,
      DATE_PART('year', date_day)                                                             AS year_actual,
      DATE_PART(quarter, date_day)                                                            AS quarter_actual,

      DATE_PART(dayofweek, date_day) + 1                                                      AS day_of_week,
      CASE WHEN day_name = 'Sun' THEN date_day
        ELSE DATEADD('day', -1, DATE_TRUNC('week', date_day)) END                             AS first_day_of_week,

      CASE WHEN day_name = 'Sun' THEN WEEK(date_day) + 1
        ELSE WEEK(date_day) END                                                               AS week_of_year_temp, --remove this column

      CASE WHEN day_name = 'Sun' AND LEAD(week_of_year_temp) OVER (ORDER BY date_day) = '1'
        THEN '1'
        ELSE week_of_year_temp END                                                            AS week_of_year,

      DATE_PART('day', date_day)                                                              AS day_of_month,

      ROW_NUMBER() OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day)          AS day_of_quarter,
      ROW_NUMBER() OVER (PARTITION BY year_actual ORDER BY date_day)                          AS day_of_year,

      CASE WHEN month_actual < 2
        THEN year_actual
        ELSE (year_actual+1) END                                                              AS fiscal_year,
      CASE WHEN month_actual < 2 THEN '4'
        WHEN month_actual < 5 THEN '1'
        WHEN month_actual < 8 THEN '2'
        WHEN month_actual < 11 THEN '3'
        ELSE '4' END                                                                          AS fiscal_quarter,

      ROW_NUMBER() OVER (PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day)          AS day_of_fiscal_quarter,
      ROW_NUMBER() OVER (PARTITION BY fiscal_year ORDER BY date_day)                          AS day_of_fiscal_year,

      TO_CHAR(date_day, 'MMMM')                                                               AS month_name,

      TRUNC(date_day, 'Month')                                                                AS first_day_of_month,
      LAST_VALUE(date_day) OVER (PARTITION BY year_actual, month_actual ORDER BY date_day)    AS last_day_of_month,

      FIRST_VALUE(date_day) OVER (PARTITION BY year_actual ORDER BY date_day)                 AS first_day_of_year,
      LAST_VALUE(date_day) OVER (PARTITION BY year_actual ORDER BY date_day)                  AS last_day_of_year,

      FIRST_VALUE(date_day) OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day) AS first_day_of_quarter,
      LAST_VALUE(date_day) OVER (PARTITION BY year_actual, quarter_actual ORDER BY date_day)  AS last_day_of_quarter,

      FIRST_VALUE(date_day) OVER (PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day) AS first_day_of_fiscal_quarter,
      LAST_VALUE(date_day) OVER (PARTITION BY fiscal_year, fiscal_quarter ORDER BY date_day)  AS last_day_of_fiscal_quarter,

      FIRST_VALUE(date_day) OVER (PARTITION BY fiscal_year ORDER BY date_day)                 AS first_day_of_fiscal_year,
      LAST_VALUE(date_day) OVER (PARTITION BY fiscal_year ORDER BY date_day)                  AS last_day_of_fiscal_year,

      DATEDIFF('week', first_day_of_fiscal_year, date_actual) +1                              AS week_of_fiscal_year,

      CASE WHEN EXTRACT('month', date_day) = 1 THEN 12
        ELSE EXTRACT('month', date_day) - 1 END                                               AS month_of_fiscal_year,

      LAST_VALUE(date_day) OVER (PARTITION BY first_day_of_week ORDER BY date_day)            AS last_day_of_week,

      (year_actual || '-Q' || EXTRACT(QUARTER FROM date_day))                                 AS quarter_name,

      (fiscal_year || '-' || DECODE(fiscal_quarter,
        1, 'Q1',
        2, 'Q2',
        3, 'Q3',
        4, 'Q4'))                                                                             AS fiscal_quarter_name,  
      ('FY' || SUBSTR(fiscal_quarter_name, 3, 7))                                             AS fiscal_quarter_name_fy,
      DENSE_RANK() OVER (ORDER BY fiscal_quarter_name)                                        AS fiscal_quarter_number_absolute,
      fiscal_year || '-' || MONTHNAME(date_day)                                               AS fiscal_month_name,
      ('FY' || SUBSTR(fiscal_month_name, 3, 8))                                               AS fiscal_month_name_fy,

      holidays.name                                                                           AS holiday_name,
      holidays.region                                                                         AS holiday_region,
      holidays.name is not null                                                               AS is_holiday,
      DATE_TRUNC('month', last_day_of_fiscal_quarter)                                         AS last_month_of_fiscal_quarter,
      IFF(DATE_TRUNC('month', last_day_of_fiscal_quarter) = date_actual, TRUE, FALSE)         AS is_first_day_of_last_month_of_fiscal_quarter,
      DATE_TRUNC('month', last_day_of_fiscal_year)                                            AS last_month_of_fiscal_year,
      IFF(DATE_TRUNC('month', last_day_of_fiscal_year) = date_actual, TRUE, FALSE)            AS is_first_day_of_last_month_of_fiscal_year

    FROM date_spine
    LEFT JOIN {{ ref('holidays') }} on holidays.date::date = date_spine.date_day

), dates as (
  SELECT 
    date_day,
    date_actual as date,
    day_name,
    month_actual,
    year_actual,
    quarter_actual,
    day_of_week,
    first_day_of_week,
    week_of_year,
    day_of_month,
    day_of_quarter,
    day_of_year,
    fiscal_year,
    fiscal_quarter,
    day_of_fiscal_quarter,
    day_of_fiscal_year,
    month_name,
    first_day_of_month,
    last_day_of_month,
    first_day_of_year,
    last_day_of_year,
    first_day_of_quarter,
    last_day_of_quarter,
    first_day_of_fiscal_quarter,
    last_day_of_fiscal_quarter,
    first_day_of_fiscal_year,
    last_day_of_fiscal_year,
    week_of_fiscal_year,
    month_of_fiscal_year,
    last_day_of_week,
    quarter_name,
    fiscal_quarter_name,
    fiscal_quarter_name_fy,
    fiscal_quarter_number_absolute,
    fiscal_month_name,
    fiscal_month_name_fy,
    holiday_name,
    holiday_region,
    is_holiday,
    last_month_of_fiscal_quarter,
    is_first_day_of_last_month_of_fiscal_quarter,
    last_month_of_fiscal_year,
    is_first_day_of_last_month_of_fiscal_year
  FROM calculated
 )

select * from dates
