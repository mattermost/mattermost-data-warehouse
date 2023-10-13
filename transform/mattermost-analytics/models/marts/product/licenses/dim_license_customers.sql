select
    distinct
    {{ dbt_utils.generate_surrogate_key(['cws_customer_id', 'license_id']) }} as customer_id,
    customer_name as name,
    customer_email as email,
    company_name as company
from
    {{ ref('int_active_licenses') }}

union

-- Handle non-CWS licenses - customers are not known in those cases.
-- Using `'Unknown'` instead of null to avoid having special null checking logic in queries.
select
    'Unknown' as customer_id,
    'Unknown' as name,
    'Unknown' as email,
    'Unknown' as company
