with self_hosted_servers as (
    select activity_date
        , server_id
        , license_id
        , customer_id
        , license_name
    from {{ ref('int_self_hosted_servers_spined') }}
    qualify row_number() over (partition by server_id, license_id order by activity_date desc) = 1
) select distinct a.activity_date as license_date
    , a.license_id as license_id
    , a.server_id as server_id
    , a.customer_id as customer_id
    , b.email as customer_email
    , b.company_name as company_name
    , coalesce(b.license_name, a.license_name) as license_name
from self_hosted_servers a 
left join {{ ref('int_self_hosted_licenses')}} b 
on a.license_id = b.license_id
where company_name is not null or email is not null