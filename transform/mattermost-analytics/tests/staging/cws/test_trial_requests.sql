
select
    *
from
    {{ source('cws', 'trial_requests') }}
where
    not {{ validate_email('contactemail')}}
    or not {{ validate_email('email')}}