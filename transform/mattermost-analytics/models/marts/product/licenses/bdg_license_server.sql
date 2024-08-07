-- Bridge table to map license ID to server ID
with filtered_licenses as (

    select
        license_id
    from
        {{ ref('int_server_license_daily') }}
    group by license_id
    -- Filter out licenses with ambiguous data
    having
        count(distinct licensed_seats) = 1
        and count(distinct starts_at) = 1
        and count(distinct expire_at) = 1
        and count(distinct customer_id) = 1

)
select
    {{ dbt_utils.generate_surrogate_key(['l.license_id', 'l.server_id', 'l.installation_id']) }} as bdg_license_server_id
    , l.license_id
    , l.server_id
    , l.installation_id
    , min(l.license_telemetry_date) as first_telemetry_date
    , max(l.license_telemetry_date) as last_telemetry_date
from
    {{ ref('int_server_license_daily') }} l
    join filtered_licenses fl on l.license_id = fl.license_id
group by l.license_id, l.server_id, l.installation_id