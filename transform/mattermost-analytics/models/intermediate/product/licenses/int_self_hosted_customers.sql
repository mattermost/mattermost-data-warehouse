-- Temporarily materialize during development
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

select distinct a.license_id
    , a.server_id
    , a.customer_id
    , b.email as customer_email
    , coalesce(a.edition, b.edition) as edition
    , b.source 
from {{ ref('int_licenses_self_hosted')}} a 
left join {{ ref('int_license_customers')}} b 
on a.license_id = b.license_id or a.customer_id = b.customer_id