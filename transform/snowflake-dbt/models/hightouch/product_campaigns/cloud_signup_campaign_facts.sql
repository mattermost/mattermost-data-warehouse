{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with signup_pages as (
    select
        daily_website_traffic.context_traits_portal_customer_id as portal_customer_id,
        min(case when name = 'pageview_verify_email' then timestamp else null end) as submitted_form_at,
        min(case when name = 'pageview_company_name' then timestamp else null end) as verified_email_at,
        min(case when name = 'pageview_create_workspace' then timestamp else null end) as entered_company_name_at
    from
        {{ ref('daily_website_traffic') }}
    where daily_website_traffic.name in
            (
                'pageview_verify_email',
                'pageview_company_name',
                'pageview_create_workspace'
            )
    group by 1
), created_workspace as (
    select
        portal_events.context_traits_portal_customer_id as portal_customer_id,
        min(timestamp) as workspace_provisioning_started_at
    from
        {{ ref('portal_events') }}
    where event = 'workspace_provisioning_started'
    group by 1
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
), customer_facts as (
    select distinct
        customers.cws_customer as portal_customer_id,
        customers.id as stripe_customer_id,
        subscriptions.cws_dns as dns,
        customers.name as company_name,
        customers.email
    from {{ ref('customers') }}
        left join {{ ref('subscriptions') }}
            on customers.id = subscriptions.customer
                and subscriptions.cws_installation is not null
)
select
    signup_pages.portal_customer_id,
    signup_pages.submitted_form_at,
    signup_pages.verified_email_at,
    signup_pages.entered_company_name_at,
    created_workspace.workspace_provisioning_started_at,
    completed_signup.completed_signup_at,
    customer_facts.stripe_customer_id,
    customer_facts.dns,
    customer_facts.company_name,
    customer_facts.email
from signup_pages
    left join created_workspace on signup_pages.portal_customer_id = created_workspace.portal_customer_id
    left join completed_signup on signup_pages.portal_customer_id = completed_signup.portal_customer_id
    left join customer_facts on signup_pages.portal_customer_id = customer_facts.portal_customer_id
