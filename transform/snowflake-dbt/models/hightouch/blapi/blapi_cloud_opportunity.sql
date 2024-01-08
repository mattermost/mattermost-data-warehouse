{{config({
    "schema": "hightouch",
    "materialized": "view",
    "tags":["hourly", "blapi", "deprecated"]
  })
}}

WITH cloud_opportunity_to_sync AS (
    SELECT DISTINCT
        account_sfid,
        account_external_id,
        opportunity_sfid,
        opportunity_external_id,
        end_date,
        ownerid,
        order_type,
        stagename,
        subscription_id,
        true as true_boolean,
        line1,
        line2,
        street_address,
        postal_code,
        city,
        state,
        country,
        state_code,
        country_code,
        'Monthly Billing' AS opportunity_type,
        SPLIT_PART(cloud_dns, '.', 0) ||
            ' Cloud Billing FY' ||
            TO_VARCHAR(end_date::date, 'yy') AS opportunity_name
    FROM {{ ref('blapi_cloud_opportunitylineitem') }}
)
SELECT * FROM cloud_opportunity_to_sync
WHERE opportunity_sfid is null