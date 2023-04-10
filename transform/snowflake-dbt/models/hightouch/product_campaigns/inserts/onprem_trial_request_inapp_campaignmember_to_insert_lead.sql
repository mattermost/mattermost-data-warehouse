{{config({
    "materialized": 'view',
    "schema": "hightouch"
  })
}}

with campaignmembers_to_insert as (
    select * from {{ ref('onprem_trial_request_inapp_facts') }}
    where
        -- Existing lead but no campaign member
        campaignmember_sfid is null and lead_exists
        -- Exceptions
        and contact_external_id not in (
            '01de907e-ca85-57ff-b7be-e0840f8bfd1f'  -- https://mattermost.atlassian.net/browse/MM-51945
        )
)
select * from campaignmembers_to_insert