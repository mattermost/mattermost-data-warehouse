{{
    config({
        "materialized": "table",
    })
}}

with license_spine as (
    select
        distinct license_id, customer_id, 'Rudderstack' as source
    from
        {{ ref('stg_mm_telemetry_prod__license') }}
    where
        license_id is not null and installation_id is null

    union

    select distinct license_id, customer_id, 'Segment' as source from  {{ ref('stg_mattermost2__license') }}

    union

    select distinct license_id, customer_id, 'CWS' as source from {{ ref('stg_cws__license') }}

    union

    select distinct license_id, customer_id, 'Legacy Licenses' as source from {{ ref('stg_licenses__licenses') }}
), onprem_servers as (
    -- On prem licenses
    select distinct
        spine.customer_id,
        spine.license_id,
        coalesce(c.customer_id, legacy.stripe_customer_id) as stripe_customer_id,
        coalesce(rudder_license.server_id, segment_license.server_id) as server_id,
        array_agg(spine.source) within group (order by spine.source) as sources
    from
        license_spine as spine
        left join {{ ref('stg_mm_telemetry_prod__license') }} rudder_license on spine.license_id = rudder_license.license_id
        left join {{ ref('stg_mattermost2__license') }} segment_license on spine.license_id = segment_license.license_id
        left join {{ ref('stg_stripe__customers') }} c on spine.customer_id = c.portal_customer_id
        left join {{ ref('stg_licenses__licenses') }} legacy on legacy.license_id = spine.license_id
    group by 1, 2, 3, 4
), cloud_spine as (
    select
        installation_id,
        'Rudderstack' as source
    from
        {{ ref('stg_mm_telemetry_prod__activity') }}
    where
        -- Not all servers are cloud servers, so ignore the ones without installation ids
        installation_id is not null

    union

    select
        cws_installation as installation_id,
        'Stripe' as source
    from
        {{ ref('stg_stripe__subscriptions') }}
    where
        cws_installation is not null
), cloud_servers as (
    -- Cloud installations
    select distinct
        c.portal_customer_id as customer_id,
        c.customer_id as stripe_customer_id,
        spine.installation_id as installation_id,
        s.cws_dns as installation_hostname,
        srv.server_id,
        array_agg(spine.source) within group (order by spine.source) as sources
    from
        cloud_spine spine
        left join {{ ref('stg_mm_telemetry_prod__server') }} srv on srv.installation_id = spine.installation_id
        left join {{ ref('stg_stripe__subscriptions') }} s on s.cws_installation = spine.installation_id
        left join {{ ref('stg_stripe__customers') }} c on s.customer_id = c.customer_id
    group by 1, 2, 3, 4, 5
)
select
    customer_id,
    stripe_customer_id,
    server_id,
    license_id,
    null as installation_id,
    null as installation_hostname,
    'Self-hosted' as type,
    source
from
    onprem_servers

union all

select
    customer_id,
    stripe_customer_id,
    server_id,
    null as license_id,
    installation_id,
    installation_hostname,
    'Cloud' as type,
    source
from
    cloud_servers