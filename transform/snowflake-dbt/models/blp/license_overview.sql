{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH licenses_old AS (
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
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),

licenses_new AS (
SELECT 
    COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)                                        AS licenseid,
    COALESCE(c.name, c.metadata:"company"::VARCHAR, SPLIT_PART(c.email, '@', 2)::VARCHAR)       AS company,
        c.id AS stripe_id,
        datediff(day, s.current_period_start, s.current_period_end::date) AS licenselength,
        c.created::date as issuedat,
        s.current_period_end::date as expiresat,
        COALESCE(c.metadata:"id"::VARCHAR, c.metadata:"cws-customer")                           AS customer_id,
        c.email as license_email,
        COALESCE(master_account.name, account.name) as master_account_name, 
        COALESCE(master_account.sfid, account.sfid) as master_account_sfid,
        account.name as account_name, 
        account.sfid as account_sfid,
        opportunity.name as opportunity_name, 
        opportunity.sfid as opportunity_sfid,
        contact.email as contact_email,
        contact.sfid as contact_sfid
FROM {{ source('stripe_raw', 'subscriptions') }} s
JOIN {{ source('stripe_raw', 'customers') }} c ON s.customer = c.id
LEFT JOIN {{ ref( 'opportunity') }} ON opportunity.license_key__c = COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)
LEFT JOIN {{ ref( 'account') }} ON account.sfid = opportunity.accountid
LEFT JOIN {{ ref( 'account') }} AS master_account ON master_account.sfid = account.parentid
LEFT JOIN {{ ref( 'contact') }} ON c.email = contact.email AND contact.accountid = account.sfid
WHERE COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL) NOT IN (SELECT licenseid FROM licenses_old GROUP BY 1)
  AND COALESCE(s.metadata:"sku"::VARCHAR, SPLIT_PART(s.plan:"name"::VARCHAR, ' ', 2) || ' Trial') IN
      ('E20', 'E10', 'E20 Trial', 'E10 Trial')
  AND s.status NOT IN ('incomplete_expired')
  AND COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL) IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
),

license_overview AS (
  SELECT 
    *
  FROM licenses_old

  UNION ALL

  SELECT
    *
  FROM licenses_new
)

SELECT * FROM license_overview
WHERE licenseid is not null