with server_telemetry_summary as (
    select
        server_id,
        min(activity_date) over (partition by server_id) as first_activity_date,
        max(activity_date) over (partition by server_id ) as last_activity_date,
        first_value(binary_edition) over (partition by server_id order by activity_date asc) as first_binary_edition,
        first_value(binary_edition) over (partition by server_id order by activity_date asc) as last_binary_edition
    from
        {{ ref('int_server_active_days_spined') }}
    -- Keep only one row per server as the current query creates duplicates
    qualify row_number() over (partition by server_id order by server_id) = 1
), user_telemetry_summary as (
    select
        server_id,
        min(activity_date) as first_activity_date,
        max(activity_date) as last_activity_date
    from
        {{ ref('int_user_active_days_spined') }}
    where
        is_active_today
    group by
        server_id
), server_info as (
   select
       coalesce(st.server_id, ut.server_id) as server_id,
       case
           when st.first_activity_date is null then ut.first_activity_date
           when ut.first_activity_date is null then st.first_activity_date
           else least(st.first_activity_date, ut.first_activity_date)
        end as first_activity_date,
       case
           when st.last_activity_date is null then ut.last_activity_date
           when ut.last_activity_date is null then st.last_activity_date
           else greatest(st.last_activity_date, ut.last_activity_date)
        end as last_activity_date,
       st.first_binary_edition as first_binary_edition,
       st.last_binary_edition as last_binary_edition
    from
        server_telemetry_summary st
        full outer join user_telemetry_summary ut on st.server_id = ut.server_id
)
select
    si.server_id,
    ht.hosting_type,
    ht.installation_id,
    ht.cloud_hostname,
    si.first_activity_date,
    si.last_activity_date,
    si.first_binary_edition,
    si.last_binary_edition
from
    server_info si
    left join {{ ref('int_server_hosting_type') }} ht on si.server_id = ht.server_id
