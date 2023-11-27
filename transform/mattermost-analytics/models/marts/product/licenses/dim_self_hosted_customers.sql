select server_id as server_id
    , license_id as license_id
    , customer_id as customer_id
    , customer_email as customer_email
    , company_name as company_name
    , license_name as license_name
    , max(activity_date) as max_activity_date
from {{ ref('int_self_hosted_servers_spined')}} 
where company_name is not null or email is not null
group by server_id 
, license_id 
, customer_id 
, email
, company_name
, license_name
