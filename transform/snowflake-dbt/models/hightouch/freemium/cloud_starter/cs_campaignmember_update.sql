{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaignmembers_to_update as (
    select * from {{ ref('cs_signup_campaign') }}
    where campaignmember_sfid is not null
)
select * from campaignmembers_to_update