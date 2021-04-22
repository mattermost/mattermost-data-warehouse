{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','contact'))}}
WHERE NOT isdeleted