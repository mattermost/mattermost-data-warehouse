with deduped_trial_requests as (
    -- Deduplicate CWS licenses due to stitch duplications
    select
        {{ dbt_utils.star(ref('stg_cws__trial_requests')) }}
    from
        {{ ref('stg_cws__trial_requests')}}
    qualify row_number() over (partition by trial_request_id order by start_at asc) = 1
), aggregates as (
    select
        coalesce(contact_email, email) as trial_email
        , count(distinct trial_request_id) as total_trial_requests
        , min(start_at) as first_trial_start_at
        , max(start_at) as last_trial_start_at
    from
        deduped_trial_requests
    group by all
)
select
    tr.trial_request_id
    , coalesce(tr.contact_email, tr.email) as trial_email
    , split_part(trial_email, '@', 2) as email_domain
    , tr.email
    , tr.contact_email
    , {{ validate_email('trial_email') }} as is_valid_trial_email
    , tr._name as name
    , tr.company_size_bucket
    , tr.server_id
    , tr.site_name
    , tr.site_url
    , tr.start_at
    , tr.end_at
    , coalesce(tr.country_name, 'Unknown') as country_name
    , tr.num_users
    , row_number() over(partition by trial_email order by tr.start_at asc) = 1 as is_first_trial
    , row_number() over(partition by trial_email order by tr.start_at desc) = 1 as is_last_trial
    , agg.total_trial_requests
    , agg.first_trial_start_at
    , agg.last_trial_start_at
from
    deduped_trial_requests tr
    left join aggregates agg on coalesce(tr.contact_email, tr.email) = agg.trial_email
