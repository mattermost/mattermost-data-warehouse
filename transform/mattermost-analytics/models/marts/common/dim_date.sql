{% set days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] %}

select
    date_day::date as date_day

    , dayname(date_day) as day_name
    , last_day(date_day, 'month') as last_day_of_month
    , week(date_day) as week_actual
    , date_part('month', date_day) as month_actual
    , date_part('year', date_day) as year_actual
    , date_part(quarter, date_day) as quarter_actual
    -- Flags
    , date_day = last_day(date_day::date, 'week') as is_last_day_of_week
    , date_day = last_day_of_month as is_last_day_of_month
    {% for day in days %}
    , date_day = case
        when date_trunc('month', date_day) = date_trunc('month', current_date) and dayname(current_date) = '{{ day }}' then current_date
        when date_trunc('month', date_day) = date_trunc('month', current_date) and dayname(current_date) <> '{{ day }}' then previous_day(current_date, '{{ day }}')
        else previous_day(last_day_of_month, '{{ day }}')
    end as is_last_{{ day.lower() }}_of_month
    {% endfor %}
from
    {{ ref('telemetry_days') }}