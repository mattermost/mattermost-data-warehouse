with deduped_trial_requests as (
    -- Deduplicate CWS licenses due to stitch duplications
    select
        {{ dbt_utils.star(ref('stg_cws__trial_requests')) }}
    from
        {{ ref('stg_cws__trial_requests')}}
    qualify row_number() over (partition by trial_request_id order by start_at asc) = 1
)
select
    -- Normalize columns appearing both in on-prem and cloud trials.
    'cws:' || tr.trial_request_id as trial_request_id
    , tr.server_id
    , null as installation_id
    , lower(coalesce(tr.contact_email, tr.email)) as trial_email
    , tr.contact_email
    , tr.email as user_email
    , split_part(trial_email, '@', 2) as email_domain
    , {{ validate_email('trial_email') }} as is_valid_trial_email
    , tr.extracted_first_name as first_name
    , tr.extracted_last_name as last_name
    , coalesce(company_name, 'Unknown') as company_name
    , tr.site_url
    , tr.start_at::date as created_at
    , tr.start_at
    , tr.end_at
    , null as stripe_product_id
    , null as converted_to_paid_at
    , null as status
    , null as license_start_at
    , null as license_end_at
    , case
        when lower(site_url) = 'https://mattermost.com' then 'Website'
        else 'In-Product'
    end as request_source
    , 'in-product' as request_type
from
    deduped_trial_requests tr
