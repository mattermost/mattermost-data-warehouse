{{
    config({
    "materialized": 'table',
    "schema": "hightouch"
    })
}}


with campaignmember_update as (
    select *
    from {{ ref('contact_in_product_trial_request') }}
    where campaignmember_sfid is not null

    union all

    select *
    from {{ ref('lead_in_product_trial_request') }}
    where campaignmember_sfid is not null
)

select * from campaignmember_update