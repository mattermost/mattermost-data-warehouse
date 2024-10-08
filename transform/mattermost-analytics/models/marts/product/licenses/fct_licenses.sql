select
    license_id
    , company_name
    , contact_email
    , sku_short_name
    , license_name
    , starts_at
    , expire_at
    , expire_at < current_date as has_expired
    , is_trial
    , licensed_seats
    , cws_licensed_seats
    , salesforce_licensed_seats
    , telemetry_licensed_seats
    , cws_starts_at
    , legacy_starts_at
    , salesforce_starts_at
    , telemetry_starts_at
    , cws_expire_at
    , legacy_expire_at
    , salesforce_expire_at
    , telemetry_expire_at
    , in_cws
    , in_legacy
    , in_salesforce
    , in_telemetry
    , salesforce_account_arr
from
    {{ ref('int_known_licenses') }}