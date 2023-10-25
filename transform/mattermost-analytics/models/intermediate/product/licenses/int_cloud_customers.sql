-- Temporarily materialize during development
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

select distinct a.installation_date
    , a.installation_id as installation_id
    , a.server_id as server_id
    , b.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
    , b.source as source
from {{ ref('int_cloud_servers')}} a 
left join {{ ref('int_cloud_licenses')}} b 
on a.installation_id = b.installation_id
where company_name is not null