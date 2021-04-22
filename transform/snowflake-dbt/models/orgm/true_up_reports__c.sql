{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','true_up_reports__c'))}}
WHERE NOT isdeleted