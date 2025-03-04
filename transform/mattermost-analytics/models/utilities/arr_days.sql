-- Dates used in ARR related calculations
{{
    dbt_utils.date_spine(
      start_date="to_date('02/01/2010', 'mm/dd/yyyy')",
      datepart="day",
      end_date="dateadd(year, 5, current_date)"
     )
}}