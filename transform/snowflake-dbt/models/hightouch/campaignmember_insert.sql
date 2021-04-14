{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with campaignmember_insert as (
    select campaignmember_dwh_external_id, license_issued_at, in_product_trial_request_campaignid as campaignid, campaign_status, contact_dwh_external_id, lead_dwh_external_id
    from {{ ref('contact_in_product_trial_request') }}
    where campaignmember_sfid is null

    union all

    select campaignmember_dwh_external_id, license_issued_at, in_product_trial_request_campaignid as campaignid, campaign_status, null, lead_dwh_external_id
    from {{ ref('lead_in_product_trial_request') }}
    where campaignmember_sfid is null
)

select * from campaignmember_insert