{{config({
    "materialized": "table",
    "schema": "blp"
  })
}}

WITH enterprise_license_mapping AS (
    SELECT 
        licenseid,
        company,
        account_sfid,
        opportunity_sfid,
        license_email,
        issuedat::timestamp as issuedat,
        expiresat::timestamp as expiresat,
        'backfill' as data_source
    FROM {{ source('staging','hist_license_mapping')}}
    WHERE NOT IGNORE AND NOT TRIAL

    UNION ALL

    SELECT 
        opportunity.license_key__c,
        COALESCE(licenses.company, licenses2.company) AS company,
        opportunity.accountid,
        opportunity.sfid,
        COALESCE(licenses.email, licenses2.email) AS email,
        COALESCE(to_timestamp(licenses.issuedat/1000)::timestamp, licenses2.issuedat::timestamp) AS issuedat,
        COALESCE(to_timestamp(licenses.expiresat/1000)::timestamp, licenses2.expiresat::timestamp) AS expiresat,
        'opportunity'
    FROM {{ ref('opportunity')}}
    LEFT JOIN {{ source('licenses', 'licenses') }} ON opportunity.license_key__c = licenses.licenseid
    LEFT JOIN (
        SELECT
            COALESCE(c.metadata:"id"::VARCHAR, c.metadata:"cws-customer")                               AS customer_id
        , COALESCE(c.name, c.metadata:"company"::VARCHAR, SPLIT_PART(c.email, '@', 2)::VARCHAR)       AS company
        , MAX(COALESCE(c.metadata:"number"::INT, c.metadata:"netsuite_customer_id"::INT))             AS number
        , c.email
        , s.id                                                                                        AS stripe_id
        , COALESCE(s.metadata:"cws-license-id"::VARCHAR, NULL)                                        AS licenseid
        , c.created                                                              AS issuedat
        , (s.current_period_end)                                                               AS expiresat
        , FALSE                                                                                       AS blapi
        , COALESCE(s.quantity, c.metadata:"seats"::INT)                                               AS users
        , COALESCE(c.metadata:"sku"::VARCHAR, SPLIT_PART(s.plan:"name"::VARCHAR, ' ', 2) || ' Trial') AS edition
        FROM {{ source('stripe_raw', 'subscriptions')}}  s
            JOIN {{ source('stripe_raw', 'customers') }} c
                ON s.customer = c.id
        WHERE COALESCE(s.metadata:"sku"::VARCHAR, SPLIT_PART(s.plan:"name"::VARCHAR, ' ', 2) || ' Trial') IN
                                ('E20', 'E10', 'E20 Trial', 'E10 Trial')
        AND s.status NOT IN ('incomplete_expired')
        GROUP BY 1, 2, 4, 5, 6, 7, 8, 9, 10, 11
     ) licenses2  ON opportunity.license_key__c = licenses2.licenseid
    WHERE NOT EXISTS (SELECT 1 FROM {{ source('staging','hist_license_mapping')}} WHERE hist_license_mapping.licenseid = opportunity.license_key__c)
        AND opportunity.license_key__c IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
select * from enterprise_license_mapping