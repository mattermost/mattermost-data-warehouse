{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with campaignmember_insert as (
    select campaignmember_dwh_external_id, in_product_trial_request_campaignid as campaignid, campaign_status, contact_dwh_external_id, lead_dwh_external_id, max(license_issued_at) as last_occurence
    from {{ ref('contact_in_product_trial_request') }}
    where campaignmember_sfid is null
    {{ dbt_utils.group_by(n=5) }}
    
    union all

    select campaignmember_dwh_external_id, in_product_trial_request_campaignid as campaignid, campaign_status, null, lead_dwh_external_id, max(license_issued_at) as last_occurence
    from {{ ref('lead_in_product_trial_request') }}
    where campaignmember_sfid is null
    {{ dbt_utils.group_by(n=5) }}
)

select * from campaignmember_insert