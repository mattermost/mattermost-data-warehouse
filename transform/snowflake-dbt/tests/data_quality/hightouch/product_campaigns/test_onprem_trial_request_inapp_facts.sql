{{ config(
    tags = ['data-quality']
) }}

select
    *
from
    {{ ref('onprem_trial_request_inapp_facts') }}
where
    not is_valid_email
