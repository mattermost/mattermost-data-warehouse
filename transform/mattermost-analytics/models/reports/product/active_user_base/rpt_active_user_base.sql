with server_dataset as (
    select
        fct_active_users.server_id,
        fct_active_users.server_monthly_active_users,
        fct_active_users.daily_active_users as client_daily_active_users,
        fct_active_users.monthly_active_users as client_monthly_active_users,
        fct_active_users.count_registered_users,
        fct_active_users.count_registered_deactivated_users,
        case
          when fct_active_users.server_monthly_active_users < 50 then '1-50'
          when (fct_active_users.server_monthly_active_users >= 50) and (fct_active_users.server_monthly_active_users < 500) THEN '50-500'
          when (fct_active_users.server_monthly_active_users >= 500) and (fct_active_users.server_monthly_active_users < 1000) THEN '500-1000'
          when fct_active_users.server_monthly_active_users >= 1000 then '>= 1000'
          else 'Unknown'
        end
        as server_mau_bucket
    from
        {{ ref('fct_active_users') }} as fct_active_users
        left join {{ ref('dim_excludable_servers') }} as dim_excludable_servers
            on fct_active_users.server_id = dim_excludable_servers.server_id
    where
        -- Keep servers with at least one user reported on the previous day. This is the last day with full data.
        fct_active_users.activity_date = dateadd(day, -1, current_date)
        and (fct_active_users.server_monthly_active_users ) > 0
        -- No exclusion reasons - no match on excludable server table
        and dim_excludable_servers.server_id is null
), last_known_info as (
    -- Last known info ONLY for servers in the dataset
    select
        si.server_id,
        si.server_ip,
        si.installation_type,
        si.binary_edition,
        si.age_in_days,
        si.activity_date
    from
        {{ ref('dim_daily_server_info') }} si
        join server_dataset sd on si.server_id = sd.server_id
    where
        si.server_ip is not null
    qualify row_number() over (partition by si.server_id order by si.activity_date desc) = 1
), distinct_ip_addresses as (
    -- Deduplicate ip addresses in order to make lookup faster
    select
        distinct server_ip
    from
        last_known_info
), last_ip_location as (
    select
        d.server_ip,
        case
            -- TODO: split 'Unknown' to use separate word for IPv6
            when parse_ip(d.server_ip, 'INET', 1):error is not null then 'Unknown'
            else coalesce(l.country_name, 'Unknown')
        end as last_known_ip_country
    from distinct_ip_addresses d
        left join {{ ref('int_ip_country_lookup') }} l
            on parse_ip(d.server_ip, 'INET', 1):ipv4 between l.ipv4_range_start and l.ipv4_range_end
)
select
    sd.server_id,
    sd.server_monthly_active_users,
    sd.client_daily_active_users,
    sd.client_monthly_active_users,
    sd.count_registered_users,
    sd.count_registered_deactivated_users,
    sd.server_mau_bucket,
    li.server_ip as last_known_server_ip,
    li.installation_type,
    li.binary_edition,
    li.age_in_days,
    ll.last_known_ip_country,
    li.activity_date as last_known_server_info_date,
    {{ dbt_utils.star(ref('dim_latest_server_customer_info'), except=['server_id'], relation_alias='dim_latest_server_customer_info') }}

from
    server_dataset sd
    left join last_known_info li on sd.server_id = li.server_id
    left join last_ip_location ll on ll.server_ip = li.server_ip
    left join {{ ref('dim_latest_server_customer_info') }} as dim_latest_server_customer_info
        on sd.server_id = dim_latest_server_customer_info.server_id
