with daily_licenses as (
    select
        server_id
        , license_telemetry_date
        , license_id
        , customer_id
        , installation_id
        , license_name
        , licensed_seats
        , issued_at
        , starts_at
        , expire_at
        , timestamp
    from
        {{ ref('stg_mm_telemetry_prod__license') }}

    union

    select
        server_id
        , license_telemetry_date
        , license_id
        , customer_id
        -- Installation ID not reported by segment
        , null as installation_id
        , license_name
        , licensed_seats
        , issued_at
        , starts_at
        , expire_at
        , timestamp
    from
        {{ ref('stg_mattermost2__license') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['server_id', 'license_telemetry_date']) }} AS daily_server_id
    , server_id
    , license_telemetry_date
    , license_id
    , customer_id
    , installation_id
    , license_name
    , licensed_seats
    , issued_at
    , starts_at
    , expire_at
    , expire_at < license_telemetry_date as has_license_expired
from
    daily_licenses
-- Keep latest license per day
qualify row_number() over (partition by server_id, license_telemetry_date order by timestamp desc) = 1
