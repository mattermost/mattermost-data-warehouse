{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_contacts as (
    select * from {{ ref('contact_us_campaign_fact') }}
    where contact_exists
)
select * from campaign_with_contacts