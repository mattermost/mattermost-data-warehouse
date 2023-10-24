select distinct a.license_date
    , a.license_id as license_id
    , a.server_id as server_id
    , a.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(a.license_name, b.license_name) as license_name
    , b.source as source
from {{ ref('int_self_hosted_servers')}} a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.license_id = b.license_id or a.customer_id = b.customer_id
where company_name is not null