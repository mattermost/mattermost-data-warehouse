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
        COALESCE(master_account.name, account.name) as master_account_name, 
        COALESCE(master_account.sfid, account.sfid) as master_account_sfid,
        account.name as account_name, 
        account.sfid as account_sfid,
        opportunity.name as opportunity_name, 
        opportunity.sfid as opportunity_sfid,
        contact.email as contact_email,
        contact.sfid as contact_sfid
FROM {{ source('licenses', 'licenses') }}
LEFT JOIN {{ ref( 'opportunity') }} ON opportunity.license_key__c = licenses.licenseid
LEFT JOIN {{ ref( 'account') }} ON account.sfid = opportunity.accountid
LEFT JOIN {{ ref( 'account') }} AS master_account ON master_account.sfid = account.parentid
LEFT JOIN {{ ref( 'contact') }} ON licenses.email = contact.email AND contact.accountid = account.sfid
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
)
SELECT * FROM license_overview