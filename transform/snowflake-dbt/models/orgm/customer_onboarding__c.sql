{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','customer_onboarding__c'))}}
WHERE NOT isdeleted