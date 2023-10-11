-- Materializing this intermediate table as it's used multiple times downstream.
{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}


select * from {{ ref('int_mattermost_nps_summary') }}
union
select * from {{ ref('int_plugin_prod_nps_summary') }}
