{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_leads as (
    select * from {{ ref('cs_signup_campaign') }}
    where lead_exists 
)
select * from campaign_with_leads