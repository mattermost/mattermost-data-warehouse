{{config({
    "materialized": "table",
    "schema": "bizops"
  })
}}

WITH data_errors AS (
    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'No Company Type' AS error_short,
        'No Company Type when owned by Brown' AS error_long
    FROM {{ source('orgm','account')}}
    JOIN {{ source('orgm','user')}} AS account_owner ON account.ownerid = account_owner.sfid
    WHERE account_owner.lastname = 'Brown'
        AND COALESCE(account.company_type__c,'') =''
    
    UNION ALL
                   
    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'Customer, no ARR' AS error_short,
        'Type = Customer, ARR = 0' AS error_long
    FROM {{ source('orgm','account')}}
    WHERE account.arr_current__c =0
        AND account.type = 'Customer'
                   
    UNION ALL
                   
    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'Owner not Rep' AS error_short,
        'Account owner System Type <> Rep' AS error_long
    FROM {{ source('orgm','account')}}
    JOIN {{ source('orgm','user')}} AS account_owner ON account.ownerid = account_owner.sfid
    WHERE account_owner.system_type__c <> 'Rep'
                   
    UNION ALL
                   
    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'No Website' AS error_short,
        'No Website,Type = Customer' AS error_long
    FROM {{ source('orgm','account')}}
    WHERE COALESCE(account.company_type__c,'') =''
        AND account.type = 'Customer'
                   
    UNION ALL
                   
    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'Dupe Domain' AS error_short,
        'Duplicate Domain (not blank)' AS error_long
    FROM {{ source('orgm','account')}}
    WHERE COALESCE(cbit__clearbitdomain__c,'') IN
        (SELECT cbit__clearbitdomain__c
         FROM {{ source('orgm','account')}}
         WHERE COALESCE(cbit__clearbitdomain__c,'')<>''
         GROUP BY 1
         HAVING count(*) > 1
        )
                   
    UNION ALL 
            
    SELECT
        'account' AS object,
        account.sfid AS object_id,
        'No CSM' AS error_short,
        'No CSM,Type = Customer' AS error_long
    FROM {{ source('orgm','account')}}
    WHERE COALESCE(account.csm_lookup__c,'') =''
        AND account.type = 'Customer'
                   
    UNION ALL 
            
    SELECT
        'contact' AS object,
        contact.sfid AS object_id,
        'No Contact Email' AS error_short,
        'No Contact Email/' || name AS error_long
    FROM {{ source('orgm','contact')}}
    WHERE
        COALESCE(email,'') = ''
                   
    UNION ALL 
            
    SELECT
        'lead' AS object,
        lead.sfid AS object_id,
        'No Email' AS error_short,
        'No Lead Email/' || name AS error_long
    FROM {{ source('orgm','lead')}}
    WHERE
      COALESCE(email,'') = '' AND
      CONVERTEDDATE IS NULL;
                       
    UNION ALL                   
           
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
        'opportunity' AS object,
        opportunity.sfid AS object_id,
        'oppt open in prior month' AS error_short,
        'oppt open in prior month/' || opportunity.name  || '/' || opportunity_owner.name AS error_long
    FROM {{ source('orgm','opportunity')}}
    JOIN {{ source('orgm','user')}} AS opportunity_owner ON opportunity.ownerid = opportunity_owner.sfid
    WHERE opportunity.status_wlo__c = 'Open' 
        AND to_char(opportunity.closedate,'YYYY-MM') < to_char(current_date,'YYYY-MM')
                   
    UNION ALL

    SELECT
        'opportunity' AS object,
        opportunity.sfid AS object_id,
        'Owner not Rep' AS error_short,
        'Oppt not owned by rep/' || opportunity.name  || '/' || opportunity_owner.name AS error_long
    FROM {{ source('orgm','opportunity')}}
    JOIN {{ source('orgm','user')}} AS opportunity_owner ON opportunity.ownerid = opportunity_owner.sfid
    WHERE opportunity_owner.system_type__c <> 'Rep'
                   
    UNION ALL

    SELECT
        'opportunity' AS object,
        opportunity.sfid AS object_id,
        'No license key' AS error_short,
        'Closed Won - No license key/' || opportunity.name  || '/' || opportunity_owner.name AS error_long
    FROM {{ source('orgm','opportunity')}}
    JOIN {{ source('orgm','user')}} AS opportunity_owner ON opportunity.ownerid = opportunity_owner.sfid
    WHERE COALESCE(opportunity.license_key__c,'') = ''
       AND opportunity.status_wlo__c = 'Won'
       AND to_char(opportunity.closedate, 'YYYY-MM') >= '2018-02'
)

SELECT * FROM data_errors
