{{config({
    "materialized": "table",
    "schema": "bizops"
  })
}}

WITH data_errors AS (
    SELECT
        'opportunity' AS object,
        opportunity.sfid AS object_id,
        'oppt CSM <> acct csm' AS error_short,
        'oppt CSM <> acct csm on open oppts, override not set' AS error_long
    FROM {{ source('orgm','account')}}
    JOIN {{ source('orgm','opportunity')}} ON opportunity.accountid = account.sfid
    WHERE opportunity.status_wlo__c = 'Open'
        AND opportunity.csm_owner__c <> account.csm_lookup__c
    
    UNION ALL

    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'No Company Type' AS error_short,
        'No Company Type when owned by Brown' AS error_long
    FROM {{ source('orgm','account')}}
    JOIN {{ source('orgm','user')}} AS account_owner ON account.ownerid = account_owner.sfid
    WHERE account_owner.lastname = 'Brown'
    
    UNION ALL

    SELECT
        'opportunity' AS object,
        opportunity.sfid AS object_id,
        'oppt open in prior month' AS error_short,
        'oppt open in prior month/' || o.name  || '/' || u.name AS error_long
    FROM {{ source('orgm','opportunity')}}
    JOIN {{ source('orgm','user')}} AS opportunity_owner ON opportunity.ownerid = opportunity_owner.sfid
    WHERE opportunity.status_wlo__c = 'Open' 
        AND to_char(opportunity.closedate,'YYYY-MM') < to_char(NOW(),'YYYY-MM')
)

SELECT * FROM data_errors