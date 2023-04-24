{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with leads_to_insert as (
    select * from {{ ref('onprem_trial_request_inapp_facts') }}
    where
        not lead_exists and not contact_exists
        -- Invalid emails will fail to sync anyway
        and is_valid_email
        -- Exceptions
        and lead_external_id not in (
            '1492e438-bcda-58a6-906c-5b45fa292ffd'  -- https://mattermost.atlassian.net/browse/MM-51946
        )
)
select * from leads_to_insert