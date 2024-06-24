with source as (
    select * from {{ source('geolite', 'country_ipv4') }}
),
renamed as (
    select
        network
        , geoname_id
        , registered_country_geoname_id
        , represented_country_geoname_id
        -- Ignored (deprecated)
        -- , is_anonymous_proxy
        -- , is_satellite_provider
        -- Ignored (always null)
        -- , is_anycast
    from source
)

select * from renamed
