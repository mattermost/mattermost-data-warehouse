{{
    config({
        "tags":"hourly",
    })
}}

{{ join_tracks_event_tables('incident_response_prod', columns=var('base_event_columns')) }}