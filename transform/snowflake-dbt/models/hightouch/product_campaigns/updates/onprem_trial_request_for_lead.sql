{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with onprem_trial_request_for_lead as (
    select * from {{ ref('onprem_trial_request_facts') }}
    where is_lead and not is_contact and not already_customer
)
select * from onprem_trial_request_for_lead