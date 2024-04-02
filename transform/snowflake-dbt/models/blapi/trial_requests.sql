{{config({
    "materialized": "incremental",
    "schema": "blapi",
    "unique_key":"license_id",
    "tags":["hourly","blapi", "deprecated"]
  })
}}

WITH lead AS (
  SELECT *,
  ROW_NUMBER() OVER (PARTITION BY email ORDER BY CREATEDDATE ASC) AS rank
  FROM {{ ref('lead') }}
),

 trial_requests AS (
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
      trial_requests.license_issued_at,
      trial_requests.receive_emails_accepted,
      trial_requests.terms_accepted,
      COALESCE(contact.sfid,lead.sfid) AS sfid,
      contact.sfid IS NOT NULL AS is_contact,
      lead.sfid IS NOT NULL AS is_lead,
      REGEXP_LIKE(UPPER(trial_requests.email),'^[A-Z0-9._%+-/!#$%&''*=?^_`{|}~]+@[A-Z0-9.-]+\\.[A-Z]{2,4}$') as valid_email
    FROM {{ source('blapi', 'trial_requests') }}
    LEFT JOIN lead ON trial_requests.email = lower(lead.email) AND lead.rank = 1
    LEFT JOIN {{ ref( 'contact') }} ON trial_requests.email = lower(contact.email)
    {% if is_incremental() %}

      WHERE license_issued_at >= (SELECT MAX(license_issued_at) FROM {{this}})

    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY trial_requests.id ORDER BY trial_requests.license_issued_at) = 1
)

SELECT * FROM trial_requests