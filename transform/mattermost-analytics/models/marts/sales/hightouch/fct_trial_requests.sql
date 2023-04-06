select
    trial_request_id,
    -- TODO: decide name
    -- TODO: finalize how
    company_size_bucket,
    company_name,
    COALESCE(contact_email, email) AS email,
    country_name,
    start_date AS trial_start_date,
    num_users
from
    {{ ref('stg_cws__trial_requests' )}}