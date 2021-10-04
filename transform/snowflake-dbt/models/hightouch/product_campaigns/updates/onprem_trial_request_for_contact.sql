{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with onprem_trial_request_for_contact as (
    select * from {{ ref('onprem_trial_request_facts') }}
    where is_contact and not already_customer
)
select * from onprem_trial_request_for_contact