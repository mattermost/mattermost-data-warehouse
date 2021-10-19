{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaignmembers_to_insert as (
    select * from {{ ref('onprem_trial_request_inapp_facts') }}
    where campaignmember_sfid is null and not lead_exists and contact_exists
)
select * from campaignmembers_to_insert