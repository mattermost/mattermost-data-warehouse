{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_contacts as (
    select * from {{ ref('freemium_signup_campaign') }}
    where contact_exists
)
select * from campaign_with_contacts