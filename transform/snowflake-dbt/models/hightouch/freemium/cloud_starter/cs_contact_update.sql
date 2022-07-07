{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_contacts as (
    select * from {{ ref('cs_signup_campaign') }}
    where contact_exists
)
select * from campaign_with_contacts