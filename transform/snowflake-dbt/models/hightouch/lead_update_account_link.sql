{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with lead_update_account_link as (
    select lead.sfid, coalesce(existing_account__c,account.sfid) as account_sfid
    from {{ ref('lead') }}
    left join {{ ref('account')}} on account.cbit__clearbitdomain__c = split_part(email,'@',2) and converteddate is null and existing_account__c is null
    where 
        (lead.existing_account__c) 
        is distinct from 
        (coalesce(lead.existing_account__c,account.sfid))
)

select * from lead_update_account_link