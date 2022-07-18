{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_sso_leads as (
    select * from {{ ref('cs_sso_facts') }}
    where not lead_exists 
)
select * from campaign_with_sso_leads