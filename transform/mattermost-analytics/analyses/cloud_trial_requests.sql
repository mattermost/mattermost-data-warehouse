with customers as (
    SELECT
        customer_id,
        email,
        name,
        contact_first_name,
        contact_last_name,
        portal_customer_id
    FROM
        {{ ref('stg_stripe__customers') }}
    where
        created_at >= '2023-04-27' -- only select customers after the release.
),
subscriptions as (
    select
        subscription_id,
        cws_installation,
        cws_dns,
        customer_id,
        trial_start_at,
        trial_end_at,
        product_id,
        created_at
    from
        {{ ref('stg_stripe__subscriptions') }}
), cloud_trial_requests as (
    select
        'stripe:' || subscriptions.subscription_id as trial_request_id,
        subscriptions.created_at::date as trial_created_at,
        customers.email,
        customers.contact_first_name,
        customers.contact_last_name,
        customers.name as company_name,
        customers.portal_customer_id,
        subscriptions.trial_start_at,
        subscriptions.trial_end_at,
        subscriptions.cws_installation as installation_id,
        subscriptions.cws_dns
    from
        customers
        -- Will lead to rows fanning out since a customer can have many subscriptions
        left join subscriptions on subscriptions.customer_id = customers.customer_id
    where
        -- Only get trial subscriptions
        subscriptions.trial_start_at is not null
        -- Only get cloud subscriptions
        AND cws_installation is not null
)
select
    t.trial_request_id,
    t.trial_created_at,
    t.contact_first_name,
    t.contact_last_name,
    t.company_name,
    t.email,
    a.name as account_name,
    l.company_type__c as company_type,
    l.lead_id,
    l.is_deleted,
    l.converted_contact_id,
    coalesce(a.account_id, ca.account_id) as account_id,
    coalesce(a.parent_id, ca.parent_id) is not null as has_parent_account
from
    cloud_trial_requests t
    left join {{ ref('stg_salesforce__lead') }} l on t.email = l.email
    left join {{ ref('stg_salesforce__account') }} a on l.converted_account_id = a.account_id
    left join {{ ref('stg_salesforce__contact') }} c on l.converted_contact_id = c.contact_id
    left join {{ ref('stg_salesforce__account') }} ca on c.account_id = ca.account_id
where
    not (
        t.email ilike any ('%@mattermost.com', '%@wearehackerone.com')
    )