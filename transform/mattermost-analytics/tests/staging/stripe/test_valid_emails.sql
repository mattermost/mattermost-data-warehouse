{{ config(
    severity = 'warn',
    error_if = '> 0'
) }}

select
    email,
    email is null
    or email = ''
    or {{validate_email('email')}} as is_valid_email,
from
    {{ source('', 'trial_requests') }}
where
    not is_valid_email
    or not is_valid_email