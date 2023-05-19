-- Warn until the duplicates are fixed
{{ config(
    severity = 'warn',
    error_if = '> 260'
) }}

select
    email, count(contact_id)
from {{ ref('stg_salesforce__contact')}}
group by email
having count(contact_id) > 1