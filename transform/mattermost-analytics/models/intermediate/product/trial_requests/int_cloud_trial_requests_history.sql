with customers as (
    SELECT
        customer_id
        , email
        , name
        , contact_first_name
        , contact_last_name
        , portal_customer_id
    FROM
        {{ ref('stg_stripe__customers') }}
    where
        created_at >= '2023-04-27' -- only select customers after the release.
),
subscriptions as (
    select
        subscription_id
        , cws_installation
        , cws_dns
        , customer_id
        , trial_start_at
        , trial_end_at
        , product_id as stripe_product_id
        , created_at
        , converted_to_paid_at
        , status
    from
        {{ ref('stg_stripe__subscriptions') }}
)
select
    'stripe:' || subscriptions.subscription_id as trial_request_id
    , customers.email as trial_email
    , customers.email as contact_email
    , null as user_email
    , split_part(trial_email, '@', 2) as email_domain
    , {{ validate_email('trial_email') }} as is_valid_trial_email
    , customers.contact_first_name as first_name
    , customers.contact_last_name as last_name
    , customers.name as company_name
    , subscriptions.cws_dns as site_url
    , subscriptions.created_at::date as created_at
    , subscriptions.trial_start_at as start_at
    , subscriptions.trial_end_at as end_at
    , subscriptions.stripe_product_id as stripe_product_id
    , subscriptions.converted_to_paid_at as converted_to_paid_at
    , subscriptions.status as status
    , 'Stripe' as request_source
    , 'cloud' as request_type
from
    customers
    -- Will lead to rows fanning out since a customer can have many subscriptions
    left join subscriptions on subscriptions.customer_id = customers.customer_id
where
    -- Only get trial subscriptions
    subscriptions.trial_start_at is not null
    -- Only get cloud subscriptions
    AND cws_installation is not null
