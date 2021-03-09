{{
    config(
        {
            "materialized": "table",
            "schema": "orgm"
        }
    )
}}

{{ latest_record (source('orgm_raw','netsuite_conn__netsuite_financial__c'))}}
WHERE NOT isdeleted