with user_telemetry_summary as (
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
       st.last_server_ip
    from
        {{ ref('int_server_telemetry_summary') }} st
        full outer join user_telemetry_summary ut on st.server_id = ut.server_id
), latest_license as (
    select
        server_id,
        license_id
    from
        {{ ref('int_server_license_daily') }}
    where
        license_id is not null
    qualify row_number() over(partition by server_id order by license_telemetry_date desc) = 1
), latest_stripe_customer as (
    select
        sub.cws_installation,
        cus.name
    from
        {{ ref('stg_stripe__subscriptions') }} sub
        left join {{ ref('stg_stripe__customers') }} cus on sub.customer_id = cus.customer_id
    qualify row_number() over(partition by cws_installation order by sub.created_at desc) = 1
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
    si.last_server_ip,
    coalesce(ip.last_known_ip_country, 'Unknown') as last_known_ip_country,
    case
        when ht.hosting_type = 'Self-hosted' then coalesce(k.company_name, 'Unknown')
        when ht.hosting_type = 'Cloud' then coalesce(cus.name, 'Unknown')
        when ht.hosting_type = 'Unknown' then  coalesce(k.company_name, cus.name, 'Unknown')
    end as company_name
from
    server_info si
    left join {{ ref('int_server_hosting_type') }} ht on si.server_id = ht.server_id
    left join {{ ref('int_server_ip_to_country') }} ip on si.server_id = ip.server_id
    left join latest_license ll on si.server_id = ll.server_id
    left join {{ ref('int_known_licenses') }} k on ll.license_id = k.license_id
    left join latest_stripe_customer cus on ht.installation_id = cus.cws_installation