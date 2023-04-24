{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with existing_members as (
    select
        campaignmember.sfid,
        campaignmember.email,
        campaignmember.dwh_external_id__c,
        row_number() over (partition by campaignmember.email order by createddate desc) as row_num
    from {{ ref('campaignmember') }}
    where campaignmember.campaignid = '7013p000001NkNoAAK'
), existing_leads as (
    select
        lead.sfid,
        lead.email,
        lead.dwh_external_id__c,
        row_number() over (partition by lead.email order by createddate desc) as row_num
    from {{ ref('lead') }}
), existing_contacts as (
    select
        contact.sfid,
        contact.email,
        contact.dwh_external_id__c,
        row_number() over (partition by contact.email order by createddate desc) as row_num
    from {{ ref('contact') }}
), trial_facts as (
    select
        tr.email
        , {{ validate_email('tr.email') }} as is_valid_email
        , left(tr.email, 40) as first_name
        -- Keep leftmost part
        , left(split_part(tr.email, '@', 1), 40) as email_prefix
        , tr.site_url
        , tr.license_id
        , tr.sfid
        , tr.users as num_users
        , date_trunc(day, start_date) as start_date
        , date_trunc(day, end_date) as end_date
        , case
            when tr.site_url = 'https://mattermost.com'
            then '[Not Provided]'
            else tr.site_url
          end as site_name
        , row_number() over (partition by tr.email order by tr.start_date desc) as row_number
        , coalesce(contact.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', tr.email)) AS contact_external_id
        , coalesce(lead.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', tr.email)) AS lead_external_id
        , coalesce(campaignmember.dwh_external_id__c, UUID_STRING('78157189-82de-4f4d-9db3-88c601fbc22e', tr.email)) AS campaignmember_external_id
        , '0051R00000GnvhhQAB' as lead_ownerid
        , campaignmember.sfid as campaignmember_sfid
        , lead.sfid as lead_sfid
        , contact.sfid as contact_sfid
        , lead.sfid is not null as lead_exists
        , contact.sfid is not null as contact_exists
        , '7013p000001NkNoAAK' as campaign_sfid
        , 'Enterprise Trial' as lead_action
        , 'In-Product Trial Request' as action_detail
        , 'Referral' as lead_source
        , 'MM Product' as lead_source_detail
        , false as marketing_suspend
        , 'Responded' as campaignmember_status
    from
        {{ ref('trial_requests') }} tr
        left join existing_members as campaignmember
            on tr.email = campaignmember.email and campaignmember.row_num = 1
        left join existing_leads as lead
            on tr.email = lead.email and lead.row_num = 1
        left join existing_contacts as contact
            on tr.email = contact.email and contact.row_num = 1
    where
        tr.email not in
            ('evan+mm@evanrush.com', 'admin@b.c', 'success+sysadmin@simulator.amazonses.com', 'test@bigbang.dev',
            'test@test.com', 'admin@b.c', 'admin@admin.com', 'admin@admin', 'test@test.test', 'admin@test.com',
            'admin@mattermost.local')
        and tr.email not like '%@test.com'
        and tr.email not like '%mattermost.com'
        and tr.site_url not like '%localhost%'
        and tr.site_url not in ('https://rc.test.mattermost.com', 'https://postgres.test.mattermost.com', 'https://jasonblais2.test.mattermost.cloud', 'http://localhost:8065', 'https://mattermost.com')
        and tr.start_date >= '2021-09-01'
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
select trial_facts.*
from
    trial_facts
    left join already_customer on trial_facts.email = already_customer.email
where
    already_customer.email is null and trial_facts.row_number = 1