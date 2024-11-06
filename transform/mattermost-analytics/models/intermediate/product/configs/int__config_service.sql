{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

{{
    dbt_utils.union_relations(
        relations=[
            ref('int_mattermost2__config_service'),
            ref('int_mm_telemetry_prod__config_service'),
        ],
        source_column_name=None
    )
}}

