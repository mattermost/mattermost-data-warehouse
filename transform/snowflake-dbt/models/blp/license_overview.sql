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
        contact.sfid as contact_sfid,
        MAX(COALESCE(regexp_substr(ol.productcode, 'E20'), regexp_substr(ol.productcode, 'E10'))) AS edition
FROM {{ source('licenses', 'licenses') }}
LEFT JOIN {{ ref( 'opportunity') }} ON opportunity.license_key__c = licenses.licenseid
LEFT JOIN {{ ref( 'account') }} ON account.sfid = opportunity.accountid
LEFT JOIN {{ ref( 'account') }} AS master_account ON master_account.sfid = account.parentid
LEFT JOIN {{ ref( 'contact') }} ON licenses.email = contact.email AND contact.accountid = account.sfid
LEFT JOIN {{ ref('opportunitylineitem') }}    ol ON opportunity.id = ol.opportunityid AND regexp_substr(ol.productcode, 'Enterprise Edition') IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),

online_conversions AS (
  SELECT
    COALESCE(s2.metadata:"cws-license-id"::VARCHAR, NULL)                                        AS licenseid,
    COALESCE(c.company_name, a.name)       AS company,
        c.stripe_id AS stripe_id,
        datediff(day, c.created_at::date, s.end_date::date) AS licenselength,
        c.created_at::date as issuedat,
        s.end_date::date as expiresat,
        c.id                           AS customer_id,
        c.email as license_email,
        a.name as master_account_name, 
        a.id as master_account_sfid,
        a.name as account_name, 
        a.id as account_sfid,
        o.name as opportunity_name, 
        o.sfid as opportunity_sfid,
        contact.email as contact_email,
        contact.sfid as contact_sfid,
        MAX(COALESCE(regexp_substr(ol.productcode, 'E20'), regexp_substr(ol.productcode, 'E10'), 
            CASE WHEN regexp_substr(ol.productcode, '(Cloud Professional|Professional Cloud)') IS NOT NULL THEN 'Mattermost Cloud Professional' 
                 WHEN regexp_substr(ol.productcode, '(Cloud Enterprise|Enterprise Cloud)') IS NOT NULL THEN 'Mattermost Cloud Enterprise' 
                  ELSE NULL END)) AS edition
  FROM {{ ref('opportunitylineitem') }}    ol
     JOIN {{ ref('opportunity') }} o
          ON ol.opportunityid = o.id
     JOIN {{ ref('account') }} a
          ON o.accountid = a.id
     JOIN {{ ref('subscriptions_blapi') }}    s
          ON ol.subs_id__c = s.id
     JOIN {{ ref('customers_blapi') }}        c
          ON s.customer_id = c.id
     JOIN {{ source('stripe_raw', 'subscriptions') }} s2
          ON s.stripe_id = s2.id
     LEFT JOIN {{ ref( 'contact') }} ON c.email = contact.email AND contact.accountid = a.sfid
WHERE ol.subs_id__c IS NOT NULL
  AND s.cloud_installation_id IS NULL
  AND COALESCE(s2.metadata:"cws-license-id"::VARCHAR, NULL) NOT IN (SELECT licenseid FROM licenses_old GROUP BY 1)
  AND COALESCE(s2.metadata:"cws-license-id"::VARCHAR, NULL) IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
),

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
        contact.sfid as contact_sfid,
        MAX(COALESCE(regexp_substr(ol.productcode, 'E20'), regexp_substr(ol.productcode, 'E10'), 
            CASE WHEN regexp_substr(ol.productcode, '(Cloud Professional|Professional Cloud)') IS NOT NULL THEN 'Mattermost Cloud Professional' 
                 WHEN regexp_substr(ol.productcode, '(Cloud Enterprise|Enterprise Cloud)') IS NOT NULL THEN 'Mattermost Cloud Enterprise' 
                  ELSE NULL END)) AS edition
FROM {{ source('stripe_raw', 'subscriptions') }} s
JOIN {{ source('stripe_raw', 'customers') }} c ON s.customer = c.id
LEFT JOIN {{ ref( 'opportunity') }} 
ON IFF(TRIM(SPLIT_PART(opportunity.license_key__c, ',', 2)) IS NOT NULL, 
    ( 
      TRIM(SPLIT_PART(opportunity.license_key__c, ',', 1)) = 
      COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)
    OR 
      TRIM(SPLIT_PART(opportunity.license_key__c, ',', 2)) = 
      COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)
    OR 
      TRIM(SPLIT_PART(opportunity.license_key__c, ',', 3)) = 
      COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)
    ),
    opportunity.license_key__c =
    COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL) 
)
LEFT JOIN {{ ref( 'account') }} ON account.sfid = opportunity.accountid
LEFT JOIN {{ ref( 'account') }} AS master_account ON master_account.sfid = account.parentid
LEFT JOIN {{ ref( 'contact') }} ON c.email = contact.email AND contact.accountid = account.sfid
LEFT JOIN {{ ref('opportunitylineitem') }}    ol ON opportunity.id = ol.opportunityid AND regexp_substr(ol.productcode, '(Enterprise Edition|Cloud)') IS NOT NULL
WHERE COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL) NOT IN (SELECT licenseid FROM licenses_old GROUP BY 1)
  AND COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL) NOT IN (SELECT licenseid FROM online_conversions GROUP BY 1)
  AND COALESCE(s.metadata:"sku"::VARCHAR, SPLIT_PART(s.plan:"name"::VARCHAR, ' ', 2) || ' Trial') IN
      ('E20', 'E10', 'E20 Trial', 'E10 Trial')
  AND s.status NOT IN ('incomplete_expired')
  AND COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL) IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
),

cloud_conversions AS (
  SELECT
    COALESCE(s.cloud_installation_id::VARCHAR, NULL)                                        AS licenseid,
    COALESCE(c.company_name, a.name)       AS company,
        c.stripe_id AS stripe_id,
        datediff(day, c.created_at::date, s.end_date::date) AS licenselength,
        c.created_at::date as issuedat,
        s.end_date::date as expiresat,
        c.id                           AS customer_id,
        c.email as license_email,
        a.name as master_account_name, 
        a.id as master_account_sfid,
        a.name as account_name, 
        a.id as account_sfid,
        o.name as opportunity_name, 
        o.sfid as opportunity_sfid,
        contact.email as contact_email,
        contact.sfid as contact_sfid,
        MAX(COALESCE(regexp_substr(ol.productcode, 'E20'), regexp_substr(ol.productcode, 'E10'), 
            CASE WHEN regexp_substr(ol.productcode, '(Cloud Professional|Professional Cloud)') IS NOT NULL THEN 'Mattermost Cloud Professional' 
                 WHEN regexp_substr(ol.productcode, '(Cloud Enterprise|Enterprise Cloud)') IS NOT NULL THEN 'Mattermost Cloud Enterprise' 
                  ELSE NULL END)) AS edition
  FROM {{ ref('opportunitylineitem') }}    ol
     JOIN {{ ref('opportunity') }} o
          ON ol.opportunityid = o.id
     JOIN {{ ref('account') }} a
          ON o.accountid = a.id
     JOIN {{ ref('subscriptions_blapi') }}    s
          ON ol.subs_id__c = s.id
     JOIN {{ ref('customers_blapi') }}        c
          ON s.customer_id = c.id
     LEFT JOIN {{ ref( 'contact') }} ON c.email = contact.email AND contact.accountid = a.sfid
WHERE ol.subs_id__c IS NOT NULL
  AND s.cloud_installation_id IS NOT NULL
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

  UNION ALL

  SELECT
    *
  FROM cloud_conversions

  UNION all

  SELECT
    *
  FROM online_conversions
)

SELECT * FROM license_overview
WHERE licenseid is not null
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
