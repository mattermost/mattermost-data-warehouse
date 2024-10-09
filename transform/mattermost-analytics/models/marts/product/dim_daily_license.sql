select
    l.daily_server_id
    , l.license_id
    , l.customer_id
    , l.license_name
    , l.licensed_seats
    , l.issued_at
    , l.starts_at
    , l.expire_at
    , l.has_license_expired
    , k.is_trial
    , coalesce(k.company_name, 'Unknown') as company_name
    , coalesce(k.contact_email, 'Unknown') as contact_email
    , coalesce(k.sku_short_name, 'Unknown') as sku_short_name
    , coalesce(k.in_cws, false) as in_cws
    , coalesce(k.in_legacy, false) as in_legacy
    , coalesce(k.in_salesforce, false) as in_salesforce
    , k.salesforce_account_arr

    -- Metadata to be used for tests
    , l.expire_at = k.expire_at as is_matching_expiration_date
    , l.licensed_seats = k.licensed_seats as is_matching_license_seats
from
    {{ ref('int_server_license_daily') }} l
    left join {{ ref('int_known_licenses') }} k on l.license_id = k.license_id