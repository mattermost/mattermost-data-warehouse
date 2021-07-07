{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_leads as (
    select * from {{ ref('cloud_signup_campaign') }}
    where lead_exists and not contact_exists
)
select * from campaign_with_leads