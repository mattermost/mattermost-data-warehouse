{{
    config({
        "materialized": "incremental",
        "tags":"hourly",
        "schema": "event_registry",
        "incremental_strategy": "merge",
        "unique_key": ['id'],
        "merge_update_columns": ['event_count'],
        "cluster_by": ['date_received_at'],
    })
}}

{{ rudder_tracks_summary('incident_response_prod') }}