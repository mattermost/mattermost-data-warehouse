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
    Ytr.trial_request_id
    Y, tr.trial_email
    Y, tr.contact_email
    Y, tr.user_email
    Y, tr.email_domain
    Y, tr.first_name
    Y, tr.last_name
    Y, tr.company_name
    Y, tr.site_url
    Y, tr.created_at
    Y, tr.start_at
    Y, tr.end_at
    Y, tr.request_source
    Y, tr.request_type
    Y, tr.stripe_product_id
    Y, tr.converted_to_paid_at
    Y, tr.status
    Y, agg.first_trial_start_at
    Y, agg.last_trial_start_at
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