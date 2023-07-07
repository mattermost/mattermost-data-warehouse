-- Create a date spine starting on the first date that events were ingested up to (and including) today.
{% set start_date = var('telemetry_start_date') %}
{{ dbt_utils.date_spine(
    datepart="day",
    start_date=start_date,
    end_date="dateadd(day, 1, current_date)"
) }}