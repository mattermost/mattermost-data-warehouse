{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}

WITH w_end_date AS
 (
  SELECT
        opportunityid as opportunity_sfid,
        min(end_date__c) as min_end_date,
        max(end_date__c) as max_end_date,
        count(distinct end_date__c) as num_diff_end_dates
  FROM {{ source('orgm', 'opportunitylineitem') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_start_date AS
 ( SELECT
        opportunityid as opportunity_sfid,
        min(start_date__c) as min_start_date,
        max(start_date__c) as max_start_date,
        count(distinct start_date__c) as num_diff_start_dates
  FROM {{ source('orgm', 'opportunitylineitem') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), opportunity_ext AS (
   SELECT
        opportunity.sfid as opportunity_sfid,
        opportunity.accountid as accountid,
        min_end_date,
        max_end_date,
        num_diff_end_dates,
        min_start_date,
        max_start_date,
        num_diff_start_dates,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'New' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_new_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_expansion_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' AND is_prorated_expansion__c IS NULL THEN totalprice WHEN is_prorated_expansion__c IS NOT NULL THEN 12 * totalprice/term_months__c ELSE 0 END) AS sum_expansion_w_proration_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Ren' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_renewal_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Multi' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_multi_amount
    FROM {{ source('orgm', 'opportunity') }}
    LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    LEFT JOIN w_end_date ON opportunity.sfid = w_end_date.opportunity_sfid
    LEFT JOIN w_start_date ON opportunity.sfid = w_start_date.opportunity_sfid
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
)

 SELECT * FROM opportunity_ext
