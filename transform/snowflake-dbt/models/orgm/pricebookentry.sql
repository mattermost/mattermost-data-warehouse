{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','pricebookentry'))}}
WHERE NOT isdeleted