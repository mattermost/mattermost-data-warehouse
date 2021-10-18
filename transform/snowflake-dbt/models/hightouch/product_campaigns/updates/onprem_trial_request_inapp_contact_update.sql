{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with contacts_update as (
    select * from {{ ref('onprem_trial_request_inapp_facts') }}
    where contact_exists
)
select * from contacts_update