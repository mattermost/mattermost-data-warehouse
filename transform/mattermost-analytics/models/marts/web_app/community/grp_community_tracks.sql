{{
    config({
        "cluster_by": ['event_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

select
    {{ dbt_utils.star(ref('int_mm_telemetry_prod__community_tracks')) }}
    , timestamp::date as event_date
from
    {{ref('int_mm_telemetry_prod__community_tracks')}}

union all

select
    {{ dbt_utils.star(ref('int_mm_telemetry_rc__community_tracks')) }}
    , timestamp::date as event_date
from
    {{ref('int_mm_telemetry_rc__community_tracks')}}