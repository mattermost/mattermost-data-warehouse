{{
    config({
        "snowflake_warehouse": "transform_l"
    })
}}

{{ join_tracks_event_tables('mm_plugin_prod', columns=get_base_event_columns()) }}