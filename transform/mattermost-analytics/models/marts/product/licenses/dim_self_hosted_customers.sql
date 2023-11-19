with self_hosted_servers as (
    select server_id
        , license_id
        , customer_id
        , license_name
    from {{ ref('int_server_hosted_servers_spined') }}
    qualify row_number() over (partition by server_id, license_id order by activity_date desc) = 1
) select a.activity_date as license_date
    , a.license_id as license_id
    , a.server_id as server_id
    , b.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
    , b.source as source
from {{ ref('self_hosted_servers')}} a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.server_id = b.server_id
where company_name is not null or email is not null