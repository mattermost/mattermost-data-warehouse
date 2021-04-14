{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with campaignmember_insert as (
    select *
    from {{ ref('contact_in_product_trial_request') }}
    where campaignmember_sfid is null

    union all

    select *
    from {{ ref('lead_in_product_trial_request') }}
    where campaignmember_sfid is null
)

select * from campaignmember_insert