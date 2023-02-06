{{ config(
    { "materialized": "table",
    "schema": "data_quality",
    "tags": "data-quality" }
) }}

WITH cloud_professional_subscriptions AS (

    SELECT
        stage.email,
        stage.domain,
        A.sfid AS account_sfid,
        co.sfid AS contact_sfid,
        o.sfid AS opportunity_sfid,
        start_date
    FROM
        {{ ref('customers_with_cloud_paid_subs') }}
        stage
        LEFT JOIN "ANALYTICS"."ORGM"."LEAD" l
        ON l.email = stage.email
        LEFT JOIN {{ ref('contact') }}
        co
        ON co.email = stage.email
        LEFT JOIN {{ ref('account') }} A
        ON A.dwh_external_id__c = stage.account_external_id
        LEFT JOIN {{ ref('opportunity') }}
        o
        ON o.dwh_external_id__c = stage.opportunity_external_id
    WHERE
        sku = 'Cloud Professional'
)
SELECT
    *
FROM
    cloud_professional_subscriptions
