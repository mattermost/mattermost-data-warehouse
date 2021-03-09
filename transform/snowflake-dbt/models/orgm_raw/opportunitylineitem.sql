{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','opportunitylineitem'))}}
WHERE NOT isdeleted