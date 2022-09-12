{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with signup_pages_account_created as (
    select
        daily_website_traffic.context_traits_portal_customer_id as portal_customer_id,
        min(case when name = 'pageview_verify_email' or name = 'verify_email' then timestamp else null end) as account_created_at,
        min(case when name = 'pageview_create_workspace' or name = 'create_workspace' then timestamp else null end) as workspace_created_at
    from
        {{ ref('daily_website_traffic') }}
    where daily_website_traffic.name in
            (
                'pageview_verify_email',
                'pageview_create_workspace',
                'verify_email',
                'create_workspace'
            )
    group by 1
), signup_pages_enter_code as (
    select 
        context_traits_portal_customer_id as portal_customer_id,
        min(case when event = 'enter_valid_code' then timestamp else null end) as verified_email_at
    from
        {{ ref('portal_events') }}
    where event = 'enter_valid_code'
    group by 1
), signup_pages_pre as (
    select 
        signup_pages_account_created.portal_customer_id,
        signup_pages_account_created.account_created_at,
        signup_pages_account_created.workspace_created_at,
        signup_pages_enter_code.verified_email_at
    from
        signup_pages_account_created 
        left join signup_pages_enter_code on signup_pages_account_created.portal_customer_id = signup_pages_enter_code.portal_customer_id
), created_workspace as (
    select
        portal_events.context_traits_portal_customer_id as portal_customer_id,
        min(timestamp) as workspace_provisioning_started_at
    from
        {{ ref('portal_events') }}
    where event = 'workspace_provisioning_started'
    group by 1
-- ), sso_providers as (
--     select context_traits_portal_customer_id as portal_customer_id,
--     sso_provider,
--     row_number() over (partition by portal_customer_id order by sent_at desc) as row_num
--         from
--         (
--             select * from 
--         {{ ref('portal_events') }}
--         where event = 'srv_oauth_complete_success'
--         )
--         where row_num = 1
), sso_signins_pre as (
    select 
        coalesce(context_traits_portal_customer_id, portal_customer_id) as portal_customer_id,
        sso_provider,
        row_number() over (partition by portal_customer_id order by timestamp desc) as row_num
        from {{ source('raw', 'identifies') }}
        where sso_provider is not null and coalesce(context_traits_portal_customer_id, portal_customer_id) is not null
), sso_signins as (
    select * from sso_signins_pre where row_num = 1 
), sso_customers as (
    select
        portal_customer_id,
        max(sso_provider) as sso_provider,
        min(customers.created) as account_created_at,
        min(customers.created) as verified_email_at,
        min(subscriptions.created) as workspace_created_at
    from sso_signins
    join {{ ref('customers') }} on sso_signins.portal_customer_id = customers.cws_customer
    left join {{ ref('subscriptions') }} on customers.id = subscriptions.customer
    where subscriptions.cws_installation is not null
    group by 1
), signup_pages as (
    select *,
    null as sso_provider
    from signup_pages_pre where portal_customer_id not in 
    (select portal_customer_id from sso_customers)
        union
    select 
    portal_customer_id,
    to_timestamp_ntz(account_created_at),
    to_timestamp_ntz(verified_email_at),
    to_timestamp_ntz(workspace_created_at),
    sso_provider
    from sso_customers
), completed_signup as (
    select
        customers.cws_customer as portal_customer_id,
        min(cloud_pageview_events.timestamp) as completed_signup_at
    from
        {{ ref('customers') }}
        join {{ ref('subscriptions') }} on customers.id = subscriptions.customer and subscriptions.cws_installation is not null
        join {{ ref('server_fact') }} on subscriptions.cws_installation = server_fact.installation_id
        join {{ ref('cloud_pageview_events') }}
            on server_fact.server_id = cloud_pageview_events.user_id
                and cloud_pageview_events.category = 'cloud_first_user_onboarding'
                and type = 'pageview_welcome'
    group by 1
), customer_facts_pre as (
    select
        customers.cws_customer as portal_customer_id,
        customers.id as stripe_customer_id,
        subscriptions.cws_dns as dns,
        subscriptions.cws_installation as installation_id,
        subscriptions.created,
        subscriptions.trial_start,
        subscriptions.trial_end,
        coalesce(subscriptions.edition, products.name) as edition,
        customers.name as company_name,
        customers.email,
        customers.updated >= '2022-06-14' as lead_sync_eligible,
        row_number() over (partition by customers.cws_customer order by subscriptions.created desc) as row_num
    from {{ ref('customers') }}
        left join {{ ref('subscriptions') }}
            on customers.id = subscriptions.customer
                and subscriptions.cws_installation is not null
        left join {{ ref('products') }} on products.id = coalesce(subscriptions.plan:"product"::varchar, subscriptions.metadata:"current_product_id"::varchar)
        -- where lower(subscriptions.edition) = 'cloud starter' 
), customer_facts as (
    select *
    from customer_facts_pre
    where row_num = 1
)
, server_facts as (
    select
        customer_facts.portal_customer_id,
        max(last_active_date) as last_active_date,
        max(posts) as cloud_posts_total,
        max(monthly_active_users) as cloud_mau,
        max(active_users) as cloud_dau,
        max(posts_previous_day) as cloud_posts_daily
    from
        {{ ref('server_fact') }}
    join customer_facts on server_fact.installation_id = customer_facts.installation_id
    where server_fact.installation_id is not null and customer_facts.installation_id is not null
    group by 1
)
select
    signup_pages.portal_customer_id,
    signup_pages.account_created_at,
    signup_pages.verified_email_at,
    signup_pages.workspace_created_at,
    signup_pages.sso_provider,
    created_workspace.workspace_provisioning_started_at,
    completed_signup.completed_signup_at,
    customer_facts.stripe_customer_id,
    customer_facts.dns,
    customer_facts.installation_id,
    case
        when customer_facts.created > customer_facts.trial_start
        then customer_facts.created
        else customer_facts.trial_start
    end as trial_start,
    customer_facts.trial_end,
    customer_facts.company_name,
    customer_facts.email,
    customer_facts.edition,
    server_facts.last_active_date,
    server_facts.cloud_posts_total,
    server_facts.cloud_mau,
    server_facts.cloud_dau,
    server_facts.cloud_posts_daily

from signup_pages
    left join created_workspace on signup_pages.portal_customer_id = created_workspace.portal_customer_id
    left join completed_signup on signup_pages.portal_customer_id = completed_signup.portal_customer_id
    left join customer_facts on signup_pages.portal_customer_id = customer_facts.portal_customer_id
    left join server_facts on signup_pages.portal_customer_id = server_facts.portal_customer_id
    -- left join sso_providers on signup_pages.portal_customer_id = sso_providers.portal_customer_id
    where lead_sync_eligible
    and customer_facts.installation_id is not null
