-- There are a few examples in the original data. Fail if new are introduced.
{{ config(
    severity = 'warn',
    error_if = '> 5'
) }}

select
    contactemail,
    contactemail is null
    or contactemail = ''
    or {{validate_email('contactemail')}} as is_valid_contactemail,
    email,
    {{validate_email('email')}} as is_valid_email
from
    {{ source('cws', 'trial_requests') }}
where
    not is_valid_contactemail
    or not is_valid_email
