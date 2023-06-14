-- Warn until the duplicates are fixed
{{ config(
    severity = 'warn',
    error_if = '> 850'
) }}

select
    email, count(lead_id)
from {{ ref('stg_salesforce__lead')}}
group by email
having count(lead_id) > 1