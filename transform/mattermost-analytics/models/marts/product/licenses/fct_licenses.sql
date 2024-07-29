select
    license_id
    , company_name
    , contact_email
    , sku_short_name
    , license_name
    , starts_at
    , expire_at
    , expire_at > current_date as is_expired
    , is_trial
    , licensed_seats
    , cws_licensed_seats
    , opportunity_licensed_seats
    , telemetry_licensed_sesats
    , cws_starts_at
    , legacy_issued_at
    , salesforce_starts_at
    , telemetry_starts_at
    , cws_expire_at
    , legacy_expire_at
    , cws_expire_at
    , telemetry_expire_at
    , in_cws
    , in_legacy
    , in_salesforce
    , in_telemetry
from
    {{ ref('int_known_licenses') }}