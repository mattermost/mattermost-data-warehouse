select
    l.daily_server_id
    , l.server_id
    , l.license_telemetry_date
    , l.license_id
    , l.customer_id
    , l.installation_id
    , l.license_name
    , l.licensed_seats
    , l.issued_at
    , l.starts_at
    , l.expire_at
    , l.has_license_expired
    , k.is_trial
    , k.company_name
    , k.contact_email
    , k.sku_short_name
    , k.source

    -- Metadata to be used for tests
    , l.expire_at = k.expire_at as is_matching_expiration_date
    , l.licensed_seats = k.licensed_seats
from
    {{ ref('int_server_license_daily') }} l
    left join {{ ref('int_known_licenses') }} k on l.license_id = k.license_id