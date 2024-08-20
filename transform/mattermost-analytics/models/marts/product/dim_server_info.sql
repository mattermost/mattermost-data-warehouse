with server_telemetry_summary as (
    select
        server_id,
        min(activity_date) over (partition by server_id) as first_activity_date,
        max(activity_date) over (partition by server_id ) as last_activity_date,
        first_value(binary_edition) over (partition by server_id order by activity_date asc) as first_binary_edition,
        last_value(binary_edition) over (partition by server_id order by activity_date asc) as last_binary_edition,
        first_value(count_registered_active_users) over (partition by server_id order by activity_date asc) as first_count_registered_active_users,
        last_value(count_registered_active_users) over (partition by server_id order by activity_date asc) as last_count_registered_active_users,
        last_value(daily_active_users) over (partition by server_id order by activity_date asc) as last_daily_active_users,
        last_value(monthly_active_users) over (partition by server_id order by activity_date asc) as last_monthly_active_users,
        last_value(server_ip) over (partition by server_id order by activity_date asc) as last_server_ip
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
       st.last_binary_edition as last_binary_edition,
       st.first_count_registered_active_users,
       st.last_count_registered_active_users,
       st.last_daily_active_users,
       st.last_monthly_active_users,
       st.last_server_ip,
       parse_ip(st.last_server_ip, 'INET', 1) as parsed_server_ip,
        case
            when parsed_server_ip:error is null
                then parse_ip(st.last_server_ip || '/7', 'INET'):ipv4_range_start
            else null
        end as ip_bucket
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
    si.last_binary_edition,
    si.first_count_registered_active_users,
    si.last_count_registered_active_users,
    si.last_daily_active_users,
    si.last_monthly_active_users,
    si.last_server_ip
--     case
--         when si.parsed_server_ip:error is not null then 'Unknown'
--         else coalesce(l.country_name, 'Unknown')
--     end as last_known_ip_country
from
    server_info si
    left join {{ ref('int_server_hosting_type') }} ht on si.server_id = ht.server_id
--     left join {{ ref('int_ip_country_lookup') }} l
--                 on si.ip_bucket = l.join_bucket
--                     and si.parsed_server_ip:ipv4 between l.ipv4_range_start and l.ipv4_range_end