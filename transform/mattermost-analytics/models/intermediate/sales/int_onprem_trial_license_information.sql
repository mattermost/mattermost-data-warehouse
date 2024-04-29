select
    trl.license_id,
    trl.trial_request_id,
    min(tl.license_telemetry_date) as activation_date,
    max(tl.license_telemetry_date) as last_license_telemetry_date,
    array_unique_agg(server_id) as server_ids
from
    {{ ref('stg_cws__trial_request_licenses') }} trl
    left join {{ ref('stg_mm_telemetry_prod__license') }} tl on trl.license_id = tl.license_id
group by 1, 2