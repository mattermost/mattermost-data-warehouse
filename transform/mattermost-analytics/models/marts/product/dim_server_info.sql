with server_info as (
    -- Summarize server hosting type information
    select
        server_id,
        -- Aggregate is cloud in order to validate whether the server is cloud or not
        count_if(is_cloud = true) as count_is_cloud_days,
        count_if(is_cloud = false) as count_not_is_cloud_days,
        min(activity_date) as first_activity_date,
        max(activity_date) as last_activity_date
    from
         {{ ref('int_server_active_days_spined') }}
    group by
        server_id
), latest_values as (
    -- Get latest values for installation id (if exists) and full version string
    select
        server_id,
        installation_id
    from
         {{ ref('int_server_active_days_spined') }}
    -- Keep latest record per day
    qualify row_number() over (partition by server_id order by activity_date desc) = 1
), latest_cloud_subscription as (
    select
        s.cws_installation as installation_id,
        s.cws_dns as cloud_hostname
    from
        {{ ref('stg_stripe__subscriptions')}} s
    where
        s.cws_installation is not null
    -- Keep latest information per installation
    qualify row_number() over (partition by s.cws_dns order by s.created_at desc) = 1
)
select
    si.server_id,
    case
        when count_is_cloud_days > 0 and count_not_is_cloud_days = 0 then 'Cloud'
        when count_is_cloud_days = 0 and count_not_is_cloud_days > 0 then 'Self-hosted'
        else 'Unknown'
    end as hosting_type,
    l.installation_id,
    s.cloud_hostname,
    si.first_activity_date,
    si.last_activity_date
from
    server_info si
    join latest_values l on si.server_id = l.server_id
    left join latest_cloud_subscription s on l.installation_id = s.installation_id
