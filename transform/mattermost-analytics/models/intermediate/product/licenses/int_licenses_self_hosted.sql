-- Temporarily materialize during development
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with cws_licenses as (
    select distinct license_id
        , customer_id 
        , customer_email 
        , company_name 
    from {{ ref('stg_cws__license') }}
), stripe_licenses as (
    select distinct license_id
        , c.portal_customer_id as customer_id
        , c.email as email
        , c.name as company_name from {{ ref('stg_stripe__subscriptions')}}  s 
    join {{ ref('stg_stripe__customers')}}  c on s.customer_id = c.customer_id
    where license_id is not null
), legacy_licenses as (
    select distinct license_id
        , customer_id 
        , contact_email as email
        , company_name 
    from {{ ref('stg_licenses__licenses')}} 
), all_licenses as (
    select * from cws_licenses
    union
    select * from stripe_licenses
    union
    select * from legacy_licenses
) select al.license_id
    , coalesce(l1.server_id, l2.server_id) as server_id
    , al.customer_id
    , al.email
    , al.company_name 
from all_licenses al 
left join {{ ref('stg_mm_telemetry_prod__license')}} l1 on al.license_id = l1.license_id
left join {{ ref('stg_mattermost2__license')}} l2 on al.license_id = l2.license_id