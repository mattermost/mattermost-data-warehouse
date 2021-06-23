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
            WHEN COALESCE(lsf3.edition, 'E20 Trial') <> 'E20 Trial' THEN lsf3.edition
                                                                    ELSE
                CASE
                    WHEN ol.productcode IN ('Enterprise Edition E20', 'Enterprise Edition E20PS') THEN 'E20'
                    WHEN ol.productcode = 'Enterprise Edition E10'                                THEN 'E10'
                    WHEN ol.productcode = 'Enterprise Edition E10 (Non-profit) 3Y'                THEN 'E10'
                    WHEN ol.productcode = 'Support'
                                                                                                  THEN NULL END END) AS sku
  , MIN(COALESCE(lsf.server_activation_date, lsf3.server_activation_date,
                 sf.first_active_date))::DATE                                                                        AS first_telemetry_date
  , MIN(COALESCE(lsf.server_activation_date, lsf3.server_activation_date,
                 sf.first_active_date))::DATE <
    MIN(o.createddate)::DATE                                                                                         AS free_to_paid
  , MIN(o.createddate)::DATE                                                                                         AS paid_conversion_date
  , MAX(ol.end_date__c)::DATE                                                                                        AS paid_expire_date
  , MAX(COALESCE(sf.last_active_date, lsf.last_server_telemetry,
                 lsf3.last_server_telemetry))::DATE                                                                  AS last_telemetry_date
  , IFF(MAX(ol.end_date__c)::DATE < CURRENT_DATE, TRUE, FALSE)                                                       AS churned
  , COUNT(DISTINCT
          COALESCE(lsf.server_id, lsf3.server_id, sf.server_id))                                                     AS servers
  , CASE WHEN MIN(lsf.license_id) IS NOT NULL THEN TRUE ELSE FALSE END                                               AS accountid_match
  , CASE WHEN MIN(lsf3.license_id) IS NOT NULL THEN TRUE ELSE FALSE END                                              AS license_key_match
  , MIN(CASE
            WHEN CASE
                     WHEN COALESCE(lsf.edition, 'E20 Trial') = 'E20 Trial'
                                                                            THEN lsf.edition
                     WHEN COALESCE(lsf3.edition, 'E20 Trial') = 'E20 Trial' THEN lsf3.edition
                                                                            ELSE NULL END IS NOT NULL
                THEN COALESCE(lsf.issued_date, lsf3.issued_date)
                ELSE NULL END)                                                                                       AS trial_date
  , COALESCE(MIN(CASE
                     WHEN CASE
                              WHEN COALESCE(lsf.edition, 'E20 Trial') = 'E20 Trial'
                                                                                     THEN lsf.edition
                              WHEN COALESCE(lsf3.edition, 'E20 Trial') = 'E20 Trial' THEN lsf3.edition
                                                                                     ELSE NULL END IS NOT NULL
                         THEN COALESCE(lsf.issued_date, lsf3.issued_date)
                         ELSE NULL END) < MIN(o.createddate)::DATE,
             FALSE)                                                                                                  AS trial_to_paid_conversion
  , MAX(CASE WHEN a.name IN ('Hold Public') THEN TRUE ELSE FALSE END)                                                AS hold_public
  , {{ dbt_utils.surrogate_key(['o.accountid'
        , 'CASE
        WHEN a.name IN (\'Hold Public\') THEN COALESCE(lsf.license_email, c.email)
                                       ELSE a.name END'])}}                                                          AS id
FROM {{ ref('opportunity') }}                  o
     JOIN {{ ref('opportunitylineitem') }}     ol
          ON o.id = ol.opportunityid
     JOIN {{ ref('account') }}                a
          ON o.accountid = a.id
     JOIN {{ ref('opportunitycontactrole') }}  oc
          ON o.id = oc.opportunityid
     JOIN {{ ref('contact') }}                 c
          ON oc.contactid = c.id
     LEFT JOIN {{ ref('license_server_fact') }} lsf
               ON CASE
                      WHEN o.partner_name__c IS NULL AND SPLIT_PART('@', c.email, 2) != 'mattermost.com' AND
                           o.accountid = '0013p00001rIvwnAAC'
                          THEN c.email = lsf.license_email
                      WHEN o.partner_name__c IS NULL AND SPLIT_PART('@', c.email, 2) != 'mattermost.com'
                          THEN SPLIT_PART(c.email, '@', 2) = SPLIT_PART(lsf.license_email, '@', 2)
                          ELSE o.accountid = lsf.account_sfid END
     LEFT JOIN blp.license_server_fact lsf3
               ON o.license_key__c = lsf3.license_id
     LEFT JOIN mattermost.server_fact  sf
               ON COALESCE(lsf.server_id, lsf3.server_id) = sf.server_id
WHERE o.iswon
  AND o.type = 'New Subscription'
  AND CASE
          WHEN COALESCE(lsf.edition, 'E20 Trial') <> 'E20 Trial'
                                                                  THEN lsf.edition
          WHEN COALESCE(lsf3.edition, 'E20 Trial') <> 'E20 Trial' THEN lsf3.edition
                                                                  ELSE
              CASE
                  WHEN ol.productcode IN ('Enterprise Edition E20', 'Enterprise Edition E20PS') THEN 'E20'
                  WHEN ol.productcode = 'Enterprise Edition E10'                                THEN 'E10'
                  WHEN ol.productcode = 'Enterprise Edition E10 (Non-profit) 3Y'                THEN 'E10'
                  WHEN ol.productcode = 'Support'
                                                                                                THEN NULL END END !=
      'Mattermost Cloud'
{% if is_incremental() %}
    AND (
            o.accountid NOT IN (SELECT accountid from existing_conversions)
        OR 
            ol.end_date__c::date >= (SELECT max(paid_expire_date) from {{this}})
        )
{% endif %}
GROUP BY 1, 2)


SELECT * 
FROM customer_conversion_onprem