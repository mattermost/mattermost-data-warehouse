{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly"]
  })
}}

with existing_leads as (
    select
        lead.sfid,
        lead.email,
        lead.dwh_external_id__c,
        row_number() over (partition by lead.email order by createddate desc) as row_num
    from {{ ref('lead') }}
), customer_facts_pre as (
    select
        customers.cws_customer as portal_customer_id,
        customers.email,
        subscriptions.cws_installation as installation_id,
        coalesce(subscriptions.cws_installation_state, 'hibernating') as cloud_installation_state,
        customers.name as company_name,
        row_number() over (partition by customers.cws_customer order by subscriptions.created desc) as row_num
    from {{ ref('customers') }}
        left join {{ ref('subscriptions') }} as subscriptions
            on customers.id = subscriptions.customer
                and subscriptions.cws_installation is not null
), customer_facts as (
    select *
    from customer_facts_pre
    where row_num = 1
), server_facts_pre as (
    select
        customer_facts.portal_customer_id,
        max(customer_facts.email) as email,
        max(last_active_date) as last_active_date,
        max(posts) as cloud_posts_total,
        max(boards_cards) as cloud_cards_total,
        max(max_enabled_plugins) as cloud_plugins_total,
        max(cloud_installation_state) as cloud_installation_state,
        max(storage_bytes) as storage_bytes,
        max(monthly_active_users) as cloud_mau,
        max(active_users) as cloud_dau,
        max(posts_previous_day) as cloud_posts_daily
    from
        {{ ref('server_fact') }}
    join customer_facts on server_fact.installation_id = customer_facts.installation_id
    where server_fact.installation_id is not null and customer_facts.installation_id is not null
    group by 1
), server_facts as(
    select 
        {{convert_bytes('server_facts_pre.storage_bytes', 'gb', 3)}} as storage,
        server_facts_pre.*, 
        existing_leads.sfid, 
        existing_leads.dwh_external_id__c 
    from server_facts_pre 
    left join existing_leads on existing_leads.email = server_facts_pre.email
    where existing_leads.row_num = 1
    and existing_leads.sfid is not null
)
select * from server_facts

