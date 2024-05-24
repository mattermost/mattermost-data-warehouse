select
    'cws:' || tr.trial_request_id as activity_id,
    subscriptions.created_at as timestamp
    'Trial Requested' as activity,
    'cws' as source,
    l.customer_id as customer_id,
    tr.email.email,
    tr.start_at as trial_start_at,
    tr.end_at as trial_end_at,
    null as installation_id,
    server_id as server_id
from
    {{ ref('stg_cws__trial_requests') }} tr
    left join {{ ref('stg_cws__trial_request_licenses') }} trl on tr.trial_request_id = trl.trial_request_id
    left join {{ ref('stg_cws__trial_request_licenses') }} l on l.license_id = trl.license_id
