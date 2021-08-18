{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with campaignmember_update as (
    select campaignmember_sfid, campaign_status, max(license_issued_at) as last_occurence
    from {{ ref('contact_in_product_trial_request') }}
    where campaignmember_sfid is not null
    {{ dbt_utils.group_by(n=2) }}

    union all

    select campaignmember_sfid, campaign_status, max(license_issued_at) as last_occurence
    from {{ ref('lead_in_product_trial_request') }}
    where campaignmember_sfid is not null
    {{ dbt_utils.group_by(n=2) }}
)

select * from campaignmember_update