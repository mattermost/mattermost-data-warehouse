-- Create a date spine starting on the first date that notification data should start being aggregated
{{ dbt_utils.date_spine(
    datepart="hour",
    start_date="'" + var('notification_start_date') + "'",
    end_date="dateadd(day, 1, current_date)"
) }}