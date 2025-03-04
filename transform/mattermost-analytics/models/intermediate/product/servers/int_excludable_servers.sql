-- Materializing this intermediate table as it's used multiple times downstream.
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}
    with seed_file as (
    -- Servers defined in seed file
    select
        trim(server_id) as server_id,
        trim(reason) as reason
    from
        {{ ref('excludable_servers') }}
)
select * from seed_file
union all
select * from {{ ref('int_excludable_servers_invalid_security_data') }} where server_id is not null
union all
select * from {{ ref('int_excludable_servers_cloud_installations') }} where server_id is not null
union all
select * from {{ ref('int_excludable_servers_single_day_activity') }} where server_id is not null
union all
select * from {{ ref('int_excludable_servers_invalid_server_id')}} where server_id is not null
union all
select * from {{ ref('int_excludable_servers_country')}} where server_id is not null