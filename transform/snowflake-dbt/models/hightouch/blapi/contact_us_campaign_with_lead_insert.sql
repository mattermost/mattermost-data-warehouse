{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaign_with_leads as (
    select * from {{ ref('contact_us_campaign_fact') }}
    where not lead_exists and not contact_exists
    -- Insert a new lead only for the first time there was a contact
    and fact_row_number = 1
)
select * from campaign_with_leads