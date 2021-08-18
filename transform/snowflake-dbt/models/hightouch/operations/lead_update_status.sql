{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with lead_update_status as (
    select lead.sfid, 'MQL' as status
    from {{ ref('lead') }}
    left join {{ ref('campaignmember') }} on campaignmember.leadid = lead.sfid
    left join {{ ref('campaign') }} on campaign.sfid = campaignid
    left join {{ ref('account') }} on account.cleaned_up_website__c = lead.cleaned_up_website__c
    left join {{ ref('user') }} as owner on owner.sfid = account.ownerid
    where campaign.name = 'Cloud Workspace Creation'
        and (campaignmember.createddate > most_recent_mql_date__c or most_recent_mql_date__c is null)
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
)
    
select lead_update_status.*
from {{ ref('lead') }}
join lead_update_status on lead_update_status.sfid = lead.sfid
where 
    (lead.status) 
    is distinct from 
    (lead_update_status.status)
 