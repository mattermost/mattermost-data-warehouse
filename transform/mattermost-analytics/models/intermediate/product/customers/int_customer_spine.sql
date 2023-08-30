{{
    config({
        "materialized": "table",
    })
}}

with license_spine as (
    select distinct license_id, customer_id from {{ ref('stg_mm_telemetry_prod__license') }
    union
    select distinct license_id, customer_id from  {{ ref('stg_mattermost2__license') }}
    union
    select distinct license_id from {{ ref('stg_cws__license') }}
), onprem_servers as (
    -- On prem licenses
    select distinct
        spine.customer_id,
        spine.license_id,
        coalesce(rudder_license.server_id, segment_license.server_id) as server_id
    from
        license_spine as spine
        left join {{ ref('stg_mm_telemetry_prod__license') }} as rudder_license on cws_license.license_id = rudder_license.license_id
        left join {{ ref('stg_mattermost2__license') }} as segment_license on cws_license.license_id = segment_license.license_id
), cloud_servers as (
    -- Cloud installations
    select distinct
        c.portal_customer_id as customer_id,
        s.cws_installation as installation_id,
        s.cws_dns as installation_hostname,
        srv.server_id
    from
        {{ ref('stg_stripe__customers') }} c
        left join {{ ref('stg_stripe__subscriptions') }} s on s.customer_id = c.customer_id
        left join {{ ref('stg_mm_telemetry_prod__server') }} srv on srv.installation_id = s.cws_installation
    where
        cws_installation is not null
)
select
    customer_id,
    server_id,
    license_id,
    null as installation_id,
    null as installation_hostname
from
    onprem_servers

union all

select
    customer_id,
    server_id,
    null as license_id,
    installation_id,
    installation_hostname
from
    cloud_servers