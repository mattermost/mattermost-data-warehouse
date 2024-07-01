-- Temporarily materialize
{{
    config({
        "materialized": "table"
    })
}}



{% set company_types = ['SMB', 'Enterprise', 'Midmarket', 'Federal', 'Academic', 'MME', 'Non-Profit'] %}

with all_trial_requests as (

    select * from {{ ref('int_cloud_trial_requests_history') }}
    union
    select * from {{ ref('int_onprem_trial_requests_history') }}

)

select * from all_trial_requests