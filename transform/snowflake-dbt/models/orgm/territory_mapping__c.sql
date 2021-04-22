{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','territory_mapping__c'))}}
WHERE NOT isdeleted