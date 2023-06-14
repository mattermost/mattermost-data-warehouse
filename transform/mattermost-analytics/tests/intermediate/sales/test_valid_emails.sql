{{ config(
    severity = 'warn'
) }}

select
    email,
    email is null
    or email = ''
    or {{validate_email('email')}} as is_valid_email
from
    {{ ref('int_cloud_trial_requests') }}
where
    not is_valid_email