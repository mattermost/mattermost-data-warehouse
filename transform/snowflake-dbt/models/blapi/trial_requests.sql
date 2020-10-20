{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"license_id"

  })
}}

WITH trial_requests AS (
    SELECT 
      trial_requests.id as license_id,
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
      trial_requests.terms_accepted,
      COALESCE(contact.sfid,lead.sfid) AS sfid,
      contact.sfid IS NOT NULL AS is_contact,
      lead.sfid IS NOT NULL AS is_lead
    FROM {{ source('blapi', 'trial_requests') }}
    LEFT JOIN {{ source('orgm', 'lead') }} ON trial_requests.email = lead.email
    LEFT JOIN {{ source('orgm', 'contact') }} ON trial_requests.email = contact.email
    {% if is_incremental() %}

    WHERE issued_at::date >= (SELECT MAX(issued_at::date) FROM {{this}})

    {% endif %}
)
SELECT * FROM trial_requests