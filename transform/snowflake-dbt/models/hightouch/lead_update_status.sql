{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with leads_to_mql as (
    select lead.sfid, 'MQL' as status, campaignmember.createddate as most_recent_mql_date
    from {{ ref('lead') }}
    left join {{ ref('campaignmember') }} on campaignmember.leadid = lead.sfid
    left join {{ ref('campaign') }} on campaign.sfid = campaignid
    left join {{ ref('account') }} on account.cleaned_up_website__c = lead.cleaned_up_website__c
    left join {{ ref('user') }} as owner on owner.sfid = account.ownerid
    where campaign.name = 'Cloud Workspace Creation'
        and (campaignmember.createddate > most_recent_mql_date__c or most_recent_mql_date__c IS NULL)
        and (
            owner.sales_segment__c IN ('AMER_APAC', 'EMEA', 'Federal')
            OR greatest(account.numberofemployees, lead.numberofemployees) > 500
            OR lead.cleaned_up_website__c in ('mattermost.com', 'wayfx.com')
            OR lead.cleaned_up_website__c like '%.mil' OR lead.cleaned_up_website__c like '%.gov'
            OR (
                    countrycode IN ('CA', 'US')
                    AND (
                            lead.cleaned_up_website__c like '%.edu' OR
                            lower(lead.company) like '%education%' OR
                            lower(lead.company) like '%school%' OR
                            lower(lead.company) like '%university%' OR
                            lower(lead.industry_text__c) like '%academic%' OR
                            lower(lead.industry_text__c) like '%public sector%' OR
                            lower(lead.industry_text__c) like '%education%'
                        )
                )
        ) and lead.status IN ('MCL','MEL','Recycle')
), lead_update_status as (
    select lead.sfid, leads_to_mql.status
    from {{ ref('lead') }}
    left join leads_to_mql on leads_to_mql.sfid = lead.sfid
    where 
        (lead.status) 
        is distinct from 
        (leads_to_mql.status,lead.status)
)

select * from lead_update_status