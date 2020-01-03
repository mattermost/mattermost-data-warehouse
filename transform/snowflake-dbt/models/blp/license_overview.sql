{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH license_overview AS (
    SELECT 
        licenses.licenseid, 
        licenses.company, 
        licenses.stripeid,
        to_timestamp(licenses.expiresat/1000)::date - to_timestamp(licenses.issuedat/1000)::date AS licenselength,
        to_timestamp(licenses.issuedat/1000) as issuedat,
        to_timestamp(licenses.expiresat/1000) as expiresat,
        licenses.customerid, 
        licenses.email as license_email,
        COALESCE(master_account.name, account.name) as master_account.name, 
        COALESCE(master_account.sfid, account.sfid) as master_account_sfid,
        account.name as account_name, 
        account.sfid as account_sfid,
        opportunity.name as opportunity_name, 
        opportunity.sfid as opportunity_sfid,
        contact.email as contact_email,
        contact.sfid as contact_sfid
FROM {{ source('licenses', 'licenses') }}
LEFT JOIN {{ source('orgm', 'opportunity') }} ON opportunity.license_key__c = licenses.licenseid
LEFT JOIN {{ source('orgm', 'account') }} ON account.sfid = opportunity.accountid
LEFT JOIN {{ source('orgm', 'account') }} AS master_account ON master_account.sfid = account.parentid
LEFT JOIN {{ source('orgm', 'contact') }} ON licenses.email = contact.email AND contact.accountid = account.sfid
)
SELECT * FROM license_overview