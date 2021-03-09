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
        issuedat,
        expiresat,
        'backfill' as data_source
    FROM {{ source('staging','hist_license_mapping')}}
    WHERE NOT IGNORE AND NOT TRIAL

    UNION ALL

    SELECT 
        opportunity.license_key__c,
        licenses.company,
        opportunity.accountid,
        opportunity.sfid,
        licenses.email,
        to_timestamp(licenses.issuedat/1000),
        to_timestamp(licenses.expiresat/1000),
        'opportunity'
    FROM {{ ref('opportunity')}}
    LEFT JOIN {{ source('licenses', 'licenses') }} ON opportunity.license_key__c = licenses.licenseid
    WHERE NOT EXISTS (SELECT 1 FROM {{ source('staging','hist_license_mapping')}} WHERE hist_license_mapping.licenseid = opportunity.license_key__c)
        AND opportunity.license_key__c IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)
select * from enterprise_license_mapping