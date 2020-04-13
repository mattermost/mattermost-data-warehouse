{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}


WITH opportunity_ext AS (
    SELECT
        opportunity.sfid as opportunity_sfid,
        opportunity.accountid,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'New' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_new_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_expansion_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' AND is_prorated_expansion__c IS NULL THEN totalprice WHEN is_prorated_expansion__c IS NOT NULL THEN 12 * totalprice/term_months__c ELSE 0 END) AS sum_expansion_w_proration_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Ren' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_renewal_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Multi' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_multi_amount
    FROM {{ source('orgm', 'opportunity') }}
    LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    GROUP BY 1, 2
)

SELECT * FROM opportunity_ext