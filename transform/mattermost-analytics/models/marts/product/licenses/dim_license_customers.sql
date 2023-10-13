select
    distinct
    {{ dbt_utils.generate_surrogate_key(['cws_customer_id', 'license_id']) }} as customer_id,
    customer_name as name,
    customer_email as email,
    company_name as company
from
    {{ ref('int_active_licenses') }}

union

-- Handle non-CWS licenses - customers are not known in those cases
select
    'Unknown' as customer_id,
    null as name,
    null as email,
    null as company;