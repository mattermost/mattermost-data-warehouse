{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with campaignmember_update as (
    select campaignmember_sfid, license_issued_at, campaign_status
    from {{ ref('contact_in_product_trial_request') }}
    where campaignmember_sfid is not null

    union all

    select campaignmember_sfid, license_issued_at, campaign_status
    from {{ ref('lead_in_product_trial_request') }}
    where campaignmember_sfid is not null
)

select * from campaignmember_update