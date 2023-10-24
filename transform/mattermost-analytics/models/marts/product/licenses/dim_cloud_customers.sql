select distinct a.installation_date
    , a.installation_id as installation_id
    , a.server_id as server_id
    , a.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(a.edition, b.edition) as edition
    , b.source as source
from {{ ref('int_cloud_servers')}} a 
left join {{ ref('int_cloud_licenses')}} b 
on a.installation_id = b.installation_id
where company_name is not null