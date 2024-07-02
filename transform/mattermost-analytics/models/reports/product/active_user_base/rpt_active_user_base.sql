with servers as (
    -- Filter servers to narrow down to the ones useful for the report.
    -- Keep list of servers with at least one user reported on the last 30 days.
    -- Exclude servers with any exclusion reason.
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
        -- Keep servers with at least one user reported on the last 30 days.
        fct_active_users.activity_date >=  dateadd(day, -30, current_date)
        and (fct_active_users.server_monthly_active_users ) > 0
        -- Exclusion reasons - any server with any exclusion reason is excluded by default
        and dim_excludable_servers.server_id is null
    -- Keep only latest row
    qualify row_number() over (partition by fct_active_users.server_id order by fct_active_users.activity_date desc) = 1
), last_known_server_info as (
    select
        si.server_id,
        si.server_ip,
        parse_ip(si.server_ip, 'INET', 1) as parsed_server_ip,
        case
            when parsed_server_ip:error is null
                then parse_ip(si.server_ip || '/7', 'INET'):ipv4_range_start
            else null
        end as ip_bucket,
        si.installation_type,
        si.binary_edition,
        si.age_in_days,
        si.activity_date
    from
        {{ ref('dim_daily_server_info') }} si
    where
        si.server_ip is not null
    qualify row_number() over (partition by si.server_id order by si.activity_date desc) = 1
), last_known_oauth_info as (
    select
        server_id,
        activity_date,
        is_office365_enabled,
        is_google_enabled,
        is_gitlab_enabled,
        is_openid_enabled,
        is_openid_google_enabled,
        is_openid_gitlab_enabled,
        is_openid_office365_enabled
    from
        {{ ref('dim_daily_server_config') }}
    where
        has_rudderstack_telemetry_data or has_segment_telemetry_data
    qualify row_number() over (partition by server_id order by activity_date desc) = 1
)
select
    servers.*,
    last_known_server_info.server_ip as last_known_server_ip,
    case
        -- TODO: separate IPv6 from `Unknown`
        when last_known_server_info.parsed_server_ip:error is not null then 'Unknown'
        else coalesce(l.country_name, 'Unknown')
    end as last_known_ip_country,
    last_known_server_info.installation_type,
    last_known_server_info.binary_edition,
    last_known_server_info.age_in_days,
    last_known_server_info.activity_date as last_known_server_info_date,
    oauth.is_office365_enabled,
    oauth.is_google_enabled,
    oauth.is_gitlab_enabled,
    oauth.is_openid_enabled,
    oauth.is_openid_google_enabled,
    oauth.is_openid_gitlab_enabled,
    oauth.is_openid_office365_enabled,
    oauth.activity_date as last_known_oauth_info_date,
    {{ dbt_utils.star(ref('dim_latest_server_customer_info'), except=['server_id'], relation_alias='dim_latest_server_customer_info') }}
from
    servers
    left join {{ ref('dim_latest_server_customer_info') }} as dim_latest_server_customer_info
        on servers.server_id = dim_latest_server_customer_info.server_id
    left join last_known_server_info on servers.server_id = last_known_server_info.server_id
    left join {{ ref('int_ip_country_lookup') }} l
            on last_known_server_info.ip_bucket = l.join_bucket
                and last_known_server_info.parsed_server_ip:ipv4 between l.ipv4_range_start and l.ipv4_range_end
    left join last_known_oauth_info oauth on servers.server_id = oauth.server_id