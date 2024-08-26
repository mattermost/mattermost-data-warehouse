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
),
subscriptions as (
    select
        subscription_id
        , cws_installation as installation_id
        , cws_dns
        , customer_id
        , trial_start_at
        , trial_end_at
        , product_id as stripe_product_id
        , created_at
        , converted_to_paid_at
        , status
        , license_start_at
        , license_end_at
    from
        {{ ref('stg_stripe__subscriptions') }}
    where
        created_at >= '2023-04-27' -- only select subscriptions after the release.
)
select
    'stripe:' || subscriptions.subscription_id as trial_request_id
    , null as server_id
    , installation_id
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
    , subscriptions.license_start_at
    , subscriptions.license_end_at
    , 'Stripe' as request_source
    , 'cloud' as request_type
from
    subscriptions
    -- Will lead to rows fanning out since a customer can have many subscriptions
    left join customers on subscriptions.customer_id = customers.customer_id
where
    -- Only get trial subscriptions
    subscriptions.trial_start_at is not null
    -- Only get cloud subscriptions
    AND installation_id is not null
