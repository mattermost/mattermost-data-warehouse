select
    'cws:' || tr.trial_request_id as activity_id,
    tr.start_at as timestamp,
    'Trial Requested' as activity,
    'cws' as source,
    l.customer_id as customer_id,
    tr.email,
    tr.first_name as contact_first_name,
    tr.last_name as contact_last_name,
    tr.company_name as company_name,
    tr.start_at as trial_start_at,
    tr.end_at as trial_end_at,
    null as installation_id,
    server_id as server_id
from
    {{ ref('stg_cws__trial_requests') }} tr
    left join {{ ref('stg_cws__trial_request_licenses') }} trl on tr.trial_request_id = trl.trial_request_id
    left join {{ ref('stg_cws__license') }} l on l.license_id = trl.license_id
