{{ config(
    tags = ['data-quality']
) }}

select
    *
from
    {{ ref('contact_us_campaign_fact') }}
where
    not is_valid_email
