{{
    config({
        "materialized": "table",
    })
}}
select
    l.country_name
    , ip.join_bucket
    , ip.ipv4_range_start
    , ip.ipv4_range_end
from
    {{ ref('stg_geolite__ipv4_to_country') }} ip
    left join {{ ref('stg_geolite__country_locations') }} l on ip.geoname_id = l.geoname_id