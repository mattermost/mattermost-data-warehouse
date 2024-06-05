{% set company_types = ['SMB', 'Enterprise', 'Midmarket', 'Federal', 'Academic', 'MME', 'Non-Profit'] %}

with deduped_trial_requests as (
    -- Deduplicate CWS licenses due to stitch duplications
    select
        {{ dbt_utils.star(ref('stg_cws__trial_requests')) }}
    from
        {{ ref('stg_cws__trial_requests')}}
    qualify row_number() over (partition by trial_request_id order by start_at asc) = 1
), aggregates as (
    select
        coalesce(tr.contact_email, tr.email) as trial_email
        , count(distinct tr.trial_request_id) as total_trial_requests
        , min(tr.start_at) as first_trial_start_at
        , max(tr.start_at) as last_trial_start_at
        , count(distinct l.company_type__c) as num_company_types
{% for company_type in company_types %}
        , count_if(l.company_type__c = '{{ company_type }}') > 0 as marked_as_{{ company_type.lower().replace('-', '_') }}
{% endfor %}
    from
        deduped_trial_requests tr
        left join {{ ref('stg_salesforce__lead')}} l on l.email = tr.contact_email or l.email = tr.email
    where
        not l.is_deleted
    group by all
)
select
    tr.trial_request_id
    , coalesce(tr.contact_email, tr.email) as trial_email
    , tr.server_id
    , split_part(trial_email, '@', 2) as email_domain
    , tr.email
    , tr.contact_email
    , {{ validate_email('trial_email') }} as is_valid_trial_email
    , tr._name as name
    , coalesce(company_name, 'Unknown') as company_name
    , case
        when tr.company_size_bucket = '' then 'Unknown'
        else tr.company_size_bucket
    end as company_size_bucket
    , tr.site_name
    , tr.site_url
    , case
        when lower(site_url) = 'https://mattermost.com' then 'Website'
        else 'In-Product'
    end as request_source
    , tr.start_at
    , tr.end_at
    , coalesce(tr.country_name, 'Unknown') as country_name
    , tr.num_users
    , row_number() over(partition by trial_email order by tr.start_at asc) = 1 as is_first_trial
    , row_number() over(partition by trial_email order by tr.start_at desc) = 1 as is_last_trial
    , agg.total_trial_requests
    , agg.first_trial_start_at
    , agg.last_trial_start_at
    , agg.num_company_types
{% for company_type in company_types %}
    , agg.marked_as_{{ company_type.lower().replace('-', '_') }}
{% endfor %}
from
    deduped_trial_requests tr
    left join aggregates agg on coalesce(tr.contact_email, tr.email) = agg.trial_email
