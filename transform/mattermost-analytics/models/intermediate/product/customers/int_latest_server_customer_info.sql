{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}


-- Create mapping of server id to license id by keeping the last license id reported by each server.
with telemetry_licenses as (
    select
        server_id, license_id, timestamp
    from
        {{ ref('stg_mm_telemetry_prod__license') }}
    where
        license_id is not null

    union

    select
        server_id, license_id, timestamp
    from
        {{ ref('stg_mattermost2__license') }}
    where
        license_id is not null
), latest_telemetry_licenses as (
    -- Keep only the last mapping of server id to license id, even in cases where telemetry has been sent both to rudder and segment.
    select
        server_id, license_id, timestamp::date as last_license_date
    from
        telemetry_licenses
    qualify row_number() over (partition by server_id order by timestamp desc) = 1
),
-- Create mapping of server id to installation id by keeping the last installation id reported by each server.
latest_telemetry_installation_ids as (
    -- Keep
    select
        server_id, installation_id, timestamp::date as last_installation_id_date
    from
        {{ ref('stg_mm_telemetry_prod__server') }}
    where installation_id is not null
    qualify row_number() over (partition by server_id order by timestamp desc) = 1
),
-- Create a list of all servers with customer data indications
all_servers as (
    select
        coalesce(l.server_id, i.server_id) as server_id
        , l.license_id
        , l.last_license_date
        , i.installation_id
        , i.last_installation_id_date
    from
        latest_telemetry_licenses l
        full outer join latest_telemetry_installation_ids i on l.server_id = i.server_id
),
license_data as (
    -- Gather all self-hosted license data from CWS and legacy licenses.
    select
        license_id
        , company_name
        , customer_email as contact_email
        , sku_short_name
        , expire_at
        , is_trial
        , 'CWS' as source
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
        , 'Legacy' as source
    from
        {{ ref('stg_licenses__licenses') }}
), cloud_information as (
    -- Customer information from stripe. No other source for now.
    select
        s.cws_installation as installation_id
        , c.email as contact_email
        , c.name as company_name
        , coalesce(s.edition, p.name) as plan_name
        , p.sku as sku
        , 'Stripe' as source
    from
        {{ ref('stg_stripe__subscriptions') }} s
        join {{ ref('stg_stripe__customers') }}  c on s.customer_id = c.customer_id
        left join {{ ref('stg_stripe__products') }} p on coalesce(s.product_id, s.current_product_id) = p.product_id
    where
        s.cws_installation is not null
    qualify row_number() over (partition by cws_installation order by current_period_end_at desc) = 1
)
select
        s.server_id
        , s.license_id
        , s.last_license_date
        , s.installation_id
        , s.last_installation_id_date
        , l.company_name as license_company_name
        , l.contact_email as license_contact_email
        , l.sku_short_name as license_sku
        , l.is_trial
        , l.source as license_source
        , c.company_name as cloud_company_name
        , c.contact_email as cloud_contact_email
        , c.plan_name as cloud_plan_name
        , c.sku as cloud_sku
        , l.license_id is not null as found_matching_license
        , c.installation_id is not null as found_matching_stripe_entry
        , c.source as cloud_source
from
    all_servers s
    left join license_data l on s.license_id = l.license_id
    left join cloud_information c on s.installation_id = c.installation_id

