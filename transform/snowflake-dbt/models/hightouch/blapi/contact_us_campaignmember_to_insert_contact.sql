{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaignmembers_to_insert as (
    select * from {{ ref('contact_us_campaign_fact') }}
    where campaignmember_sfid is null and not lead_exists and contact_exists
    -- For each insert batch, keep the most recent record according to "request to contact us"
    qualify row_number() over (partition by email order by request_to_contact_us_date desc) = 1
)
select * from campaignmembers_to_insert