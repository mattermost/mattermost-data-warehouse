with server_activity_stats as (
    select
        server_id,
        count(distinct log_date) as total_activity_days
    from
        {{ ref('base_diagnostics__log_entries') }}
    group by server_id
)
select
    {{ dbt_utils.star(ref('base_diagnostics__log_entries')) }}
from
    {{ ref('base_diagnostics__log_entries') }}
where
    -- Exclude servers with just 1 day of activity.
    -- At the time of the implementation, there were almost 1.5m servers calling the endpoint just on a single date
    server_id not in (select server_id from server_activity_stats where total_activity_days = 1)