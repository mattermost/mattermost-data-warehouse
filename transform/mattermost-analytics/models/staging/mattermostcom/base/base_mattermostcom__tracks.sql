{{
    config({
        "tags":"hourly",
    })
}}

{{ join_tracks_event_tables('mattermostcom', columns=var('base_event_columns')) }}