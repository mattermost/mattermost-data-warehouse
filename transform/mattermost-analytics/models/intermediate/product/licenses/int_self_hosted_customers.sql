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
    , b.company_name
    , coalesce(a.edition, b.edition) as edition
    , b.source 
from {{ ref('int_self_hosted_servers')}} a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.license_id = b.license_id or a.customer_id = b.customer_id
where company_name is not null