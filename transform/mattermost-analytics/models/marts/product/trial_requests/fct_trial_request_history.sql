{% set company_types = ['SMB', 'Enterprise', 'Midmarket', 'Federal', 'Academic', 'MME', 'Non-Profit'] %}

with all_trial_requests as (

    select * from {{ ref('int_cloud_trial_requests_history') }}
    union
    select * from {{ ref('int_onprem_trial_requests_history') }}

), aggregates as (
    -- Calculate aggregates for each email appearing in trial requests.
    select
        trial_email as trial_email
        , count(distinct tr.trial_request_id) as total_trial_requests
        , min(tr.start_at) as first_trial_start_at
        , max(tr.start_at) as last_trial_start_at
        , count(distinct l.company_type__c) as num_company_types
{% for company_type in company_types %}
        , count_if(l.company_type__c = '{{ company_type }}') > 0 as marked_as_{{ company_type.lower().replace('-', '_') }}
{% endfor %}
    from
        all_trial_requests tr
        -- Join on either contact email or user email to improve join coverage.
        left join {{ ref('stg_salesforce__lead')}} l on l.email = tr.contact_email or l.email = tr.user_email
    where
        not l.is_deleted
    group by all
)
select
    tr.trial_request_id
    , tr.server_id
    , tr.installation_id
    , tr.trial_email
    , tr.contact_email
    , tr.user_email
    , tr.email_domain
    , tr.first_name
    , tr.last_name
    , tr.company_name
    , tr.site_url
    , tr.created_at
    , tr.start_at
    , tr.end_at
    , tr.request_source
    , tr.request_type
    , tr.stripe_product_id
    , tr.converted_to_paid_at
    , tr.status
    , tr.license_start_at
    , tr.license_end_at
    , agg.first_trial_start_at
    , agg.last_trial_start_at
    , agg.num_company_types
{% for company_type in company_types %}
    , agg.marked_as_{{ company_type.lower().replace('-', '_') }}
{% endfor %}
from
    all_trial_requests tr
    left join aggregates agg on tr.trial_email = agg.trial_email
where
    is_valid_trial_email
    and not tr.trial_email ilike any (
        '%mattermost.com'
        , '%wearehackerone.com'
        , '%example.com'
        , '%example.loc'
        , '%localhost'
        , '%test.com'
        , '%teste'
        , '%test.set'
        , '%test.test'
        , '%test.loc'
        , '%test.local'
        , '%test.ru'
        , '%test.email'
        , '%test.com'
        , '%test.de'
        , 'test@mail.com'
        , 'test@gmail.com'
        , 'admin@admin'
        , 'admin@mail.com'
        , '%@admin.com'
        , 'localhost@localhost.com'
        , '%.local'
        , 'root@root'
        , 'a@llsss.top'
        , '%@paulbunyan.net'
        , 'asd@asd.asd'
        , 'mm@mm.mm'
        , 'foo@foo.foo'
        , '%@a.a'
        , 'a@a.com'
        , 'a@mail.com'
        , 'test@lab.txt'
    )