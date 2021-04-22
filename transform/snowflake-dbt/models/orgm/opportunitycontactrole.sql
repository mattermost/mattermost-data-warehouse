{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','opportunitycontactrole'))}}
WHERE NOT isdeleted