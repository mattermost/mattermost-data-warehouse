{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaignmembers_to_insert as (
    select * from {{ ref('cs_signup_campaign') }}
    where campaignmember_sfid is null and lead_exists
)
select * from campaignmembers_to_insert