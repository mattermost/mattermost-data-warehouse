{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','opportunityhistory'))}}
WHERE NOT isdeleted