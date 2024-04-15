--
-- Active user base analysis, performed on 2024-04.
--

with dataset as (
    -- Use modified query from Looker to create list of servers.
    SELECT
            CASE
                WHEN fct_active_users.server_monthly_active_users < 50 THEN '< 50'
                WHEN (fct_active_users.server_monthly_active_users >= 50) AND (fct_active_users.server_monthly_active_users < 500) THEN '50-500'
                WHEN (fct_active_users.server_monthly_active_users >= 500) AND (fct_active_users.server_monthly_active_users < 1000) THEN '500-1000'
                WHEN fct_active_users.server_monthly_active_users >= 1000 THEN '>= 1000'
                ELSE 'Unknown'
            END
        AS mau_bucket
        , fct_active_users.server_id
        , fct_active_users.server_monthly_active_users
        , fct_active_users.count_registered_users
        , fct_active_users.count_registered_deactivated_users
    FROM {{ ref('fct_active_users') }}  AS fct_active_users
    LEFT JOIN {{ ref('dim_excludable_servers') }} AS dim_excludable_servers ON fct_active_users.server_id = dim_excludable_servers.server_id
    WHERE
        -- Check data on exact date only
        (fct_active_users.activity_date ) = (TO_DATE(TO_TIMESTAMP('2024-03-31')))
        -- Only consider servers that have been active in the last month
        AND (fct_active_users.server_monthly_active_users ) > 0
        -- Use all exclusion reasons
        AND (
            (dim_excludable_servers.has_reason_single_day_security_only <> 'Yes' OR dim_excludable_servers.has_reason_single_day_security_only IS NULL)
            AND (dim_excludable_servers.has_reason_single_day_server_side_telemetry_only <> 'Yes' OR dim_excludable_servers.has_reason_single_day_server_side_telemetry_only IS NULL)
            AND (dim_excludable_servers.has_reason_single_day_user_telemetry_only <> 'Yes' OR dim_excludable_servers.has_reason_single_day_user_telemetry_only IS NULL)
            AND (dim_excludable_servers.has_reason_single_day_telemetry_only <> 'Yes' OR dim_excludable_servers.has_reason_single_day_telemetry_only IS NULL)
            AND (dim_excludable_servers.has_reason_custom_build_version_format <> 'Yes' OR dim_excludable_servers.has_reason_custom_build_version_format IS NULL)
            AND (dim_excludable_servers.has_reason_ran_tests <> 'Yes' OR dim_excludable_servers.has_reason_ran_tests IS NULL)
            AND (dim_excludable_servers.has_reason_internal_email <> 'Yes' OR dim_excludable_servers.has_reason_internal_email IS NULL)
            AND (dim_excludable_servers.has_reason_test_server <> 'Yes' OR dim_excludable_servers.has_reason_test_server IS NULL)
            AND (dim_excludable_servers.has_reason_community <> 'Yes' OR dim_excludable_servers.has_reason_community IS NULL)
            AND (dim_excludable_servers.has_reason_active_users__registered_users <> 'Yes' OR dim_excludable_servers.has_reason_active_users__registered_users IS NULL)
            AND (dim_excludable_servers.has_reason_no_stripe_installation_found <> 'Yes' OR dim_excludable_servers.has_reason_no_stripe_installation_found IS NULL)
            AND (dim_excludable_servers.has_reason_restricted_ip <> 'Yes' OR dim_excludable_servers.has_reason_restricted_ip IS NULL)
            AND (dim_excludable_servers.has_reason_invalid_server_id <> 'Yes' OR dim_excludable_servers.has_reason_invalid_server_id IS NULL)
        )
),
-- Add IP address to each server using latest telemetry received
latest_ip_address as (
    select server_id, server_ip, timestamp::date as last_ip_address_date
    from {{ ref('stg_mm_telemetry_prod__server') }}
    qualify row_number() over(partition by server_id order by timestamp desc) = 1
),
-- Create mapping of server id to license id by keeping the last license id reported by each server.
rudder_licenses as (
    select
        server_id, license_id, timestamp
    from
        {{ ref('stg_mm_telemetry_prod__license') }}
    qualify row_number() over(partition by server_id order by timestamp desc) = 1
), segment_licenses as (
    select
        server_id, license_id, timestamp
    from
        {{ ref('stg_mattermost2__license') }}
    qualify row_number() over(partition by server_id order by timestamp desc) = 1
), union_licenses as (
    select * from rudder_licenses
    union
    select * from segment_licenses
), telemetry_licenses as (
    -- Keep only the last, even in cases where telemetry has been sent both to rudder and segment.
    select
        server_id, license_id
    from
        union_licenses
    qualify row_number() over(partition by server_id order by timestamp desc) = 1
), license_data as (
    -- Gather all self-hosted license data from CWS and legacy licenses.
    select
        license_id
       , company_name
       , customer_email as contact_email
       , sku_short_name
       , expire_at
       , is_trial
    from
        {{ ref('stg_cws__license') }}

    union

    select
        license_id
       , company_name
       , contact_email
       , 'Unknown' as sku_short_name
       , expire_at
       , false as is_trial
    from
        {{ ref('stg_licenses__licenses') }}
), cloud_licenses as (
    -- Cloud licenses, based on installation id.
    select
        s.cws_installation as installation_id
        , c.portal_customer_id as customer_id
        , c.email as email
        , c.name as company_name
        , coalesce(s.edition, p.name) as license_name
        , p.sku
        , 'Stripe' as source
    from
        {{ ref('stg_stripe__subscriptions') }} s
        join {{ ref('stg_stripe__customers') }}  c on s.customer_id = c.customer_id
        left join {{ ref('stg_stripe__products') }} p on coalesce(s.product_id, s.current_product_id) = p.product_id
    where
        s.cws_installation is not null
    qualify row_number() over (partition by cws_installation order by current_period_end_at desc) = 1

)
-- Glue everything together
select
    d.*,
    si.first_activity_date,
    si.last_activity_date,
    si.last_binary_edition,
    ip.server_ip,
    ip.last_ip_address_date,
    l.license_id,
    si.installation_id,
    si.hosting_type,
    coalesce(l.company_name, cl.company_name) as company_name,
    coalesce(l.contact_email, cl.email) as contact_email,
    coalesce(l.sku_short_name, cl.sku) as sku_short_name,
    l.expire_at,
    l.is_trial,
    cl.license_name as cloud_license_name
from
    dataset d
    left join mart_product.dim_server_info si on d.server_id = si.server_id
    left join latest_ip_address ip on d.server_id = ip.server_id
    left join telemetry_licenses tl on d.server_id = tl.server_id
    left join license_data l on l.license_id = tl.license_id
    left join cloud_licenses cl on si.installation_id = cl.installation_id
