-- Temporarily materialize
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{% set company_types = ['SMB', 'Enterprise', 'Midmarket', 'Federal', 'Academic', 'MME', 'Non-Profit'] %}

with deduped_trial_requests as (
    -- Deduplicate CWS licenses due to stitch duplications
    select
        {{ dbt_utils.star(ref('stg_cws__trial_requests')) }}
    from
        {{ ref('stg_cws__trial_requests')}}
    qualify row_number() over (partition by trial_request_id order by start_at asc) = 1
), aggregates as (
    -- Calculate aggregates for each email appearing in trial requests.
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
    -- Normalize columns appearing both in on-prem and cloud trials.
    tr.trial_request_id
    , coalesce(tr.contact_email, tr.email) as trial_email
    , split_part(trial_email, '@', 2) as email_domain
    , {{ validate_email('trial_email') }} as is_valid_trial_email
    , tr.extracted_first_name as first_name
    , tr.extracted_last_name as last_name
    , coalesce(company_name, 'Unknown') as company_name
    , tr.site_url
    , tr.start_at
    , tr.end_at
    , case
        when lower(site_url) = 'https://mattermost.com' then 'Website'
        else 'In-Product'
    end as request_source
    -- Aggregates
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
