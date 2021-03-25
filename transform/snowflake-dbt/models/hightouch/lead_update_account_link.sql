{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with lead_update_account_link as (
    select lead.sfid, coalesce(existing_account__c,account.sfid) as account_sfid
    from {{ ref('lead') }}
    left join {{ ref('account')}} on account.cbit__clearbitdomain__c = split_part(email,'@',2) and converteddate is null and existing_account__c is null
)

select lead_update_account_link.* 
from lead_update_account_link
join {{ ref('lead') }} on lead.sfid = lead_update_account_link.sfid
where 
  (lead.existing_account__c) 
  is distinct from 
  (coalesce(lead.existing_account__c,lead_update_account_link.account_sfid))