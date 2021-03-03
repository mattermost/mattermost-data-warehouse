{{
    config(
        {
            "materialized": "table",
            "schema": "orgm_new"
        }
    )
}}

{{ latest_record (source('orgm_raw','campaign'))}}
WHERE NOT isdeleted