update orgm.lead
set status = 'MQL',
    most_recent_mql_date__c = a.cm_created
from (
    select lead.sfid, lead.status, lead.email, campaignmember.createddate as cm_created, owner.name as account_name, greatest(account.numberofemployees,lead.numberofemployees) as employee_count
    from orgm.lead
             left join orgm.campaignmember on campaignmember.leadid = lead.sfid
             left join orgm.campaign on campaign.sfid = campaignid
             left join orgm.account on account.cleaned_up_website__c = lead.cleaned_up_website__c
             left join orgm.user as owner on owner.sfid = account.ownerid
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
) as a
where a.sfid = lead.sfid;