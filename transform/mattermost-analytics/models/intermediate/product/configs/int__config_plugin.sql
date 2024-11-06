{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mattermost2__config_plugin'),
            ref('int_mm_telemetry_prod__config_plugin'),
        ],
        source_column_name=None
    )
}}