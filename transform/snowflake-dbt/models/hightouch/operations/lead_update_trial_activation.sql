{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with lead_update_trial_activation as (
  select l.Id, l.Email, l.Trial_License_Id__c, lsf.LICENSE_ACTIVATION_DATE as Activation_Date
  from {{ ref('lead') }} l 
  left join {{ ref('license_server_fact') }} lsf on lsf.LICENSE_EMAIL = l.Email -- on lsf.LICENSE_ID = l.Trial_License_Id__c
  where lsf.LICENSE_ACTIVATION_DATE is not null and 
  l.Trial_License_Id__c is not null and
  l.Trial_Activation_Date__c is null
)

select Id, Activation_Date from lead_update_trial_activation