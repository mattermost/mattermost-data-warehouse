-- Create a date spine starting on the first date that events were ingested
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="'2020-04-01'",
    end_date="current_date"
) }}