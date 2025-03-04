-- Create mapping of server id to license id by keeping the last license id reported by each server.
with telemetry_licenses as (
    select
        server_id, license_id, timestamp, license_name
    from
        {{ ref('stg_mm_telemetry_prod__license') }}
    where
        license_id is not null

    union

    select
        server_id, license_id, timestamp, license_name
    from
        {{ ref('stg_mattermost2__license') }}
    where
        license_id is not null
), latest_telemetry_licenses as (
    -- Keep only the last mapping of server id to license id, even in cases where telemetry has been sent both to rudder and segment.
    select
        server_id, license_id, timestamp::date as last_license_telemetry_date, license_name
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
        , l.last_license_telemetry_date
        , l.license_name
        , i.installation_id
        , i.last_installation_id_date
    from
        latest_telemetry_licenses l
        full outer join latest_telemetry_installation_ids i on l.server_id = i.server_id
), cloud_information as (
    -- Customer information from stripe. No other source for now.
    select
        s.cws_installation as installation_id
        , c.email as contact_email
        , c.name as company_name
        , coalesce(s.edition, p.name) as plan_name
        , p.sku as sku
        , 'Stripe' as source
        , s.quantity as licensed_seats
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
        , s.installation_id
        , l.company_name as license_company_name
        , l.contact_email as license_contact_email
        , l.sku_short_name as license_sku
        , l.expire_at as license_expire_at
        , l.is_trial
        , c.company_name as cloud_company_name
        , c.contact_email as cloud_contact_email
        , c.plan_name as cloud_plan_name
        , c.sku as cloud_sku
        , c.source as cloud_source
        , l.license_id is not null as found_matching_license_data
        , c.installation_id is not null as found_matching_stripe_entry
        , s.last_license_telemetry_date
        , s.last_installation_id_date
        , l.licensed_seats as license_licensed_seats
        , c.licensed_seats as cloud_licensed_seats
        , s.license_name

from
    all_servers s
    left join {{ ref('int_known_licenses') }} l on s.license_id = l.license_id
    left join cloud_information c on s.installation_id = c.installation_id

