{{config({
    "materialized": "table",
    "schema": "blapi"
  })
}}

WITH trial_requests AS (
    SELECT 
        trial_requests.id,
        trial_requests.email,
        trial_requests.name,
        trial_requests.site_url,
        trial_requests.site_name,
        trial_requests.server_id,
        trial_requests.start_date,
        trial_requests.end_date,
        trial_requests.users,
        trial_requests.license_payload,
        trial_requests.license_issued_at,
        trial_requests.receive_emails_accepted,
        trial_requests.terms_accepted
    FROM {{ source('blapi','trial_requests') }}
)

SELECT * FROM trial_requests