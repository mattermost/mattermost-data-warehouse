select a.server_id as server_id
    , a.license_id as license_id
    , b.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
    , b.source as source
    , max(a.activity_date) as max_activity_date
from {{ ref('int_self_hosted_servers_spined')}} a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.license_id = b.license_id and a.customer_id = b.customer_id
where company_name is not null or email is not null
group by a.server_id 
, a.license_id 
, b.customer_id 
, b.email
, b.company_name
, license_name
, b.source 