select a.server_id as server_id
    , a.installation_id as installation_id
    , b.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
    , min(a.license_date) as first_license_telemetry_date
    , max(a.license_date) as last_license_telemetry_date
from {{ ref('int_cloud_servers')}} a 
left join {{ ref('int_cloud_licenses')}} b 
on a.installation_id = b.installation_id
where company_name is not null or email is not null
group by server_id 
, installation_id 
, customer_id 
, customer_email
, company_name
, license_name
