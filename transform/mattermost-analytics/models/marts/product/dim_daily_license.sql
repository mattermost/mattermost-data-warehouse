select
    spine.daily_server_id
    , coalesce(l.license_id, 'Unknown') as license_id
    , coalesce(l.customer_id, 'Unknown') as customer_id
    , coalesce(l.license_name, 'Unknown') as license_name
    , coalesce(l.licensed_seats, 0) as licensed_seats
    , l.issued_at
    , l.starts_at
    , l.expire_at
    , l.has_license_expired
    , k.is_trial
    , coalesce(k.company_name, 'Unknown') as company_name
    , coalesce(k.contact_email, 'Unknown') as contact_email
    , coalesce(k.sku_short_name, 'Unknown') as sku_short_name
    , coalesce(k.source, 'None') as source

    -- Metadata to be used for tests
    , l.expire_at = k.expire_at as is_matching_expiration_date
    , l.licensed_seats = k.licensed_seats as is_matching_license_seats
from
    {{ ref('int_server_active_days_spined') }} spine
    left join {{ ref('int_server_license_daily') }} l on spine.daily_server_id = l.daily_server_id
    left join {{ ref('int_known_licenses') }} k on l.license_id = k.license_id