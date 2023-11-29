select a.server_id as server_id
    , a.license_id as license_id
    , b.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
    , min(a.license_date) as first_license_telemetry_date
    , max(a.license_date) as last_license_telemetry_date
from {{ ref('int_self_hosted_servers')}} a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.license_id = b.license_id
where b.company_name is not null or b.email is not null
group by a.server_id
    , a.license_id 
    , b.customer_id
    , b.email 
    , b.company_name 
    , coalesce(b.license_name, a.license_name)
