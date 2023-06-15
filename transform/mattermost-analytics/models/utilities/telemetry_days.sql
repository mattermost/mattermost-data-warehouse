-- Create a date spine starting on the first date that events were ingested up to (and including) today.
{{ dbt_utils.date_spine(
    datepart="day",
    start_date="'2020-04-01'",
    end_date="dateadd(day, 1, current_date)"
) }}