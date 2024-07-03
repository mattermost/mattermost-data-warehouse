with source as (
    select * from {{ source('geolite', 'country_ipv4') }}
),
renamed as (
    select
        network
        , cast(split_part(network, '/', 2) as int) as cidr_network_size
        , parse_ip(parse_ip(network, 'INET'):host || '/7', 'INET'):ipv4_range_start as join_bucket
        , parse_ip(network, 'INET'):ipv4_range_start as ipv4_range_start
        , parse_ip(network, 'INET'):ipv4_range_end as ipv4_range_end
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
