{{
    config(
        {
            "materialized": "table",
            "schema": "orgm_new"
        }
    )
}}

{{ latest_record (source('salesforce_raw','Opportunity'))}}
WHERE NOT isdeleted