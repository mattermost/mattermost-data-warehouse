{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_leads as (
    select * from {{ ref('contact_us_campaign_fact') }}
    where not lead_exists and not contact_exists
)
select * from campaign_with_leads