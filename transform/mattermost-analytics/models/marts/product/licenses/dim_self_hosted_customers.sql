select {{ dbt_utils.generate_surrogate_key(['server_id', 'license_id']) }} as self_hosted_customer_id,
    , a.server_id as server_id
    , a.license_id as license_id
    , b.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
from {{ ref('int_self_hosted_servers')}} a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.license_id = b.license_id
where b.company_name is not null or b.email is not null
qualify row_number() over (partition by a.server_id order by license_telemetry_date desc) = 1
