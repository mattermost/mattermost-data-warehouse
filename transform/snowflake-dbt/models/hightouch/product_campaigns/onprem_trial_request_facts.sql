{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with latest_server_daily_details as (
    select 
        server_daily_details_ext.server_id
        , server_daily_details_ext.enable_cluster -- high availability
        , server_daily_details_ext.enable_compliance -- advanced compliance
        , server_daily_details_ext.enable_office365_oauth -- office365 suite integration
        , server_daily_details_ext.enable_saml -- saml
        , server_daily_details_ext.enable_incident_response -- incident response
        , server_daily_details_ext.experimental_ldap_group_sync -- ldap group sync
        , row_number() over (partition by server_daily_details_ext.server_id order by server_daily_details_ext.date desc) as row_number
    from {{ ref('server_daily_details_ext') }}
) , trial_facts as (
    select
        tr.email
        , tr.site_url
        , tr.license_id
        , tr.server_id
        , tr.start_date::date as start_date
        , tr.end_date::date as expire_date
        , 'E20 Trial' as edition
        , case when server_fact.max_registered_users >= 100 then 'More than 100 users; ' else '' end as num_users
        , case when coalesce(latest_server_daily_details.enable_cluster, false) then 'High Availability; ' else '' end as high_availability
        , case when coalesce(latest_server_daily_details.enable_compliance, false) then 'Advanced Compliance; ' else '' end as advanced_compliance
        , case when coalesce(latest_server_daily_details.enable_office365_oauth, false) then 'Office365 OAuth; ' else '' end as oauth365_oauth
        , case when coalesce(latest_server_daily_details.enable_saml, false) then 'Okta / OneLogin / ADFS SAML 2.0; ' else '' end as saml
        , case when coalesce(latest_server_daily_details.enable_incident_response, false) then 'Incident Response; ' else '' end as incident_response
        , case when coalesce(latest_server_daily_details.experimental_ldap_group_sync, false) then 'Active Directory / LDAP Group Sync; ' else '' end as ldap_group_sync
        , latest_server_daily_details.enable_cluster
        , latest_server_daily_details.enable_compliance
        , latest_server_daily_details.enable_office365_oauth
        , latest_server_daily_details.enable_saml
        , latest_server_daily_details.enable_incident_response
        , latest_server_daily_details.experimental_ldap_group_sync
        , tr.sfid
        , tr.is_contact
        , tr.is_lead
        , row_number() over (partition by tr.email order by tr.start_date desc) as row_number
    from 
        {{ ref('trial_requests') }} tr
        join latest_server_daily_details on tr.server_id = latest_server_daily_details.server_id
            and latest_server_daily_details.row_number = 1
        left join {{ ref('server_fact') }} on server_fact.server_id = tr.server_id
    where (
        latest_server_daily_details.enable_cluster
        or latest_server_daily_details.enable_compliance
        or latest_server_daily_details.enable_office365_oauth
        or latest_server_daily_details.enable_saml
        or latest_server_daily_details.enable_incident_response
        or latest_server_daily_details.experimental_ldap_group_sync
        or server_fact.max_registered_users >= 100
    )
        and tr.email not in
            ('evan+mm@evanrush.com', 'admin@b.c', 'success+sysadmin@simulator.amazonses.com', 'test@bigbang.dev',
            'test@test.com', 'admin@b.c', 'admin@admin.com', 'admin@admin', 'test@test.test', 'admin@test.com',
            'admin@mattermost.local')
        and tr.email not like '%mattermost.com'
        and tr.site_url not in ('https://rc.test.mattermost.com', 'https://postgres.test.mattermost.com', 'https://jasonblais2.test.mattermost.cloud')
        order by tr.email, tr.start_date desc
), days_with_trial as (
   select
    trial_facts.email,
    count(distinct dates.date) as days_with_trial
    from trial_facts
    join {{ ref('dates') }}
    on dates.date_day between trial_facts.start_date and trial_facts.expire_date
    group by 1
), already_customer as (
    select
        distinct trial_facts.email
    from trial_facts
    join {{ ref('account') }} on split_part(trial_facts.email, '@', 2) = account.cbit__clearbitdomain__c
    join {{ ref('opportunity') }} on account.id = opportunity.accountid
    join {{ ref('opportunitylineitem') }} on opportunity.id = opportunitylineitem.opportunityid
    join {{ ref('pricebookentry') }} on opportunitylineitem.pricebookentryid = pricebookentry.id
    where
          pricebookentry.name not like '%cloud%'
          and current_date() between opportunitylineitem.start_date__c and opportunitylineitem.end_date__c
)
select
   num_users || high_availability || advanced_compliance || oauth365_oauth || saml || incident_response || ldap_group_sync
    || (case when days_with_trial.days_with_trial >= 90 then 'possible trial abuse' else '' end)
   as pql_reason,
   trial_facts.*,
   already_customer.email is not null as already_customer
from trial_facts
join days_with_trial on trial_facts.email = days_with_trial.email
left join already_customer on trial_facts.email = already_customer.email
where trial_facts.row_number = 1
