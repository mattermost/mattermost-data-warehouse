{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id'
  })
}}

{% if is_incremental() %}

WITH existing_conversions AS (
    SELECT distinct blapi_subscription_id
    FROM {{ this }}
),

{% else %}

WITH

{% endif %}

customer_conversion_cloud AS (
-- Cloud Free to Paid Conversions
SELECT
    a.id AS account_id
  , a.name AS account_name
  , c.email AS customer_email
  , c.stripe_id AS stripe_customer_id
  , ol.subs_id__c AS blapi_subscription_id
  , s.stripe_id AS subscription_id
  , s.cloud_installation_id
  , {{ dbt_utils.surrogate_key(['a.id', 'a.name', 'ol.subs_id__c'])}} AS id
  , MIN(sf.first_active_date::date) AS first_active_date
  , MAX(sf.last_active_date::date) AS last_active_date
  , MIN(ol.createddate::date) AS paid_conversion_date
FROM {{ ref('opportunitylineitem') }}    ol
     JOIN {{ ref('opportunity') }} o
          ON ol.opportunityid = o.id
     JOIN {{ ref('account') }} a
          ON o.accountid = a.id
     JOIN {{ ref('subscriptions_blapi') }}    s
          ON ol.subs_id__c = s.id
     JOIN {{ ref('customers_blapi') }}        c
          ON s.customer_id = c.id
     JOIN {{ ref('server_fact') }} sf
          ON s.cloud_installation_id = sf.installation_id
WHERE ol.subs_id__c IS NOT NULL
  AND s.cloud_installation_id IS NOT NULL
{% if is_incremental() %}
AND (
        ol.subs_id__c NOT IN (SELECT blapi_subscription_id FROM existing_conversions)
    OR
        sf.last_active_date::date >= (SELECT MAX(last_active_date) FROM {{ this }})
    )
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 10 DESC
)

SELECT *
FROM customer_conversion_cloud