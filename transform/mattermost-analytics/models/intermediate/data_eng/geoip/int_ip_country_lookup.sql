{{
    config({
        "materialized": "table",
    })
}}
select
        l.country_name
       , parse_ip(network, 'INET'):ipv4_range_start as ipv4_range_start
       , parse_ip(network, 'INET'):ipv4_range_end as ipv4_range_end
    from
        {{ ref('stg_geolite__ipv4_to_country') }} ip
        left join {{ ref('stg_geolite__country_locations') }} l on ip.geoname_id = l.geoname_id