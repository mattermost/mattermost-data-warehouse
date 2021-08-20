{{config({
    "materialized": 'incremental',
    "schema": "blp",
    "unique_key": 'id'
  })
}}

{% if is_incremental() %}
WITH existing_conversions AS (
    SELECT distinct accountid
    FROM {{this}}
),

new_closed_ops AS (
    SELECT DISTINCT accountid
    FROM {{ ref('opportunity') }}
    WHERE createddate >= (SELECT MAX(last_won_opportunity_date) from {{this}})
),

{% else %}

WITH 

{% endif %}

customer_conversion_onprem AS (
-- Self-Managed Paying Customers
SELECT
    o.accountid
  , CASE
        WHEN a.name IN ('Hold Public') THEN COALESCE(lsf.license_email, c.email)
                                       ELSE a.name END                                                               AS account_name
  , MAX(CASE
            WHEN COALESCE(lsf.edition, 'E20 Trial') <> 'E20 Trial'
                                                                    THEN lsf.edition
                                                                    ELSE
                CASE
                    WHEN ol.productcode IN ('Enterprise Edition E20', 'Enterprise Edition E20PS') THEN 'E20'
                    WHEN ol.productcode = 'Enterprise Edition E10'                                THEN 'E10'
                    WHEN ol.productcode = 'Enterprise Edition E10 (Non-profit) 3Y'                THEN 'E10'
                    WHEN ol.productcode = 'Support'
                                                                                                  THEN NULL END END) AS sku
  , MIN(COALESCE(lsf.server_activation_date,
                 sf.first_active_date))::DATE                                                                        AS first_telemetry_date
  , MIN(COALESCE(lsf.server_activation_date,
                 sf.first_active_date))::DATE <
    MIN(o.createddate)::DATE                                AS free_to_paid
  , MIN(o.createddate)::DATE                                AS paid_conversion_date
  , MAX(o.createddate) AS last_won_opportunity_date
  , MAX(ol.end_date__c)::DATE                                                                                        AS paid_expire_date
  , MAX(COALESCE(sf.last_active_date, lsf.last_server_telemetry))::DATE                                                                  AS last_telemetry_date
  , IFF(MAX(ol.end_date__c)::DATE < CURRENT_DATE, TRUE, FALSE)                                                       AS churned
  , COUNT(DISTINCT
          COALESCE(lsf.server_id, sf.server_id))                                                     AS servers
  , CASE WHEN MIN(lsf.license_id) IS NOT NULL THEN TRUE ELSE FALSE END                                               AS accountid_match
  , MIN(CASE
            WHEN CASE
                     WHEN lsf.edition = 'E20 Trial'
                                                                            THEN lsf.edition
                                                                            ELSE NULL END IS NOT NULL
                THEN lsf.issued_date
                ELSE NULL END)                                                                                       AS trial_date
  , COALESCE(MIN(CASE
                     WHEN CASE
                              WHEN lsf.edition = 'E20 Trial'
                                                                                     THEN lsf.edition
                                                                                     ELSE NULL END IS NOT NULL
                         THEN lsf.issued_date
                         ELSE NULL END) < MIN(CASE WHEN o.type = 'New Subscription' THEN o.createddate ELSE NULL END)::DATE,
             FALSE)                                                                                                  AS trial_to_paid_conversion
  , MAX(CASE WHEN a.name IN ('Hold Public') THEN TRUE ELSE FALSE END)                                                AS hold_public
  , MAX(o.amount) AS amount
  , {{ dbt_utils.surrogate_key(['o.accountid'
        , 'CASE
        WHEN a.name IN (\'Hold Public\') THEN COALESCE(lsf.license_email, c.email)
                                       ELSE a.name END'])}}                                                          AS id
FROM {{ ref('opportunity') }}                  o
     JOIN {{ ref('opportunitylineitem') }}     ol
          ON o.id = ol.opportunityid
     JOIN {{ ref('account') }}                a
          ON o.accountid = a.id
     JOIN {{ ref('contact') }}                 c
          ON a.id = c.accountid
     LEFT JOIN {{ ref('license_server_fact') }} lsf
               ON CASE
                      WHEN o.partner_name__c IS NULL AND SPLIT_PART('@', c.email, 2) != 'mattermost.com'
                          THEN c.email = lsf.license_email
                          WHEN o.accountid IN ('0013p00001rIvwnAAC')
                          THEN o.license_key__c = lsf.license_id 
                            ELSE o.ACCOUNTID = lsf.account_sfid END
     LEFT JOIN mattermost.server_fact  sf
               ON lsf.server_id = sf.server_id
WHERE o.iswon
  AND o.type in ('New Subscription', 'Existing Business', 'Renewal', 'Contract Expansion', 'Account Expansion')
  AND CASE
          WHEN COALESCE(lsf.edition, 'E20 Trial') <> 'E20 Trial'
                                                                  THEN lsf.edition
                                                                  ELSE
              CASE
                  WHEN ol.productcode IN ('Enterprise Edition E20', 'Enterprise Edition E20PS') THEN 'E20'
                  WHEN ol.productcode = 'Enterprise Edition E10'                                THEN 'E10'
                  WHEN ol.productcode = 'Enterprise Edition E10 (Non-profit) 3Y'                THEN 'E10'
                  WHEN ol.productcode = 'Support'
                                                                                                THEN NULL END END !=
      'Mattermost Cloud'
{% if is_incremental() %}
    and (
            o.accountid NOT IN (SELECT accountid from existing_conversions)
        OR 
            o.accountid IN (SELECT accountid from new_closed_ops)
        )
{% endif %}
GROUP BY 1, 2
)


SELECT cco.*, MAX(lsf.server_id) as trial_server_id
FROM customer_conversion_onprem cco
LEFT JOIN {{ ref('license_server_fact') }} lsf
    ON cco.accountid = lsf.customer_id 
    AND cco.trial_date = lsf.issued_date
    AND lsf.license_priority_rank = 1
    AND lsf.edition = 'E20 Trial'
    AND lsf.server_id is not null
{{ dbt_utils.group_by(n=17)}}