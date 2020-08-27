{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}

WITH w_end_date AS (
  SELECT
        opportunityid as opportunity_sfid,
        min(end_date__c) as min_end_date,
        max(end_date__c) as max_end_date,
        count(distinct end_date__c) as num_diff_end_dates
  FROM {{ source('orgm', 'opportunitylineitem') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_start_date AS ( 
  SELECT
        opportunityid as opportunity_sfid,
        min(start_date__c) as min_start_date,
        max(start_date__c) as max_start_date,
        count(distinct start_date__c) as num_diff_start_dates
  FROM {{ source('orgm', 'opportunitylineitem') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_oppt_commit AS ( 
  SELECT
       o.sfid as opportunity_sfid,
       min(ofh.createddate) as first_commit_date,
       max(ofh.createddate) as last_commit_date,
       count(distinct(to_char(ofh.createddate,'YYYY-MM'))) as num_times_slipped
  FROM {{ source('orgm', 'opportunityfieldhistory') }} ofh
  LEFT JOIN {{ source('orgm', 'opportunity') }} o ON (ofh.opportunityid = o.sfid)
  WHERE ofh.field = 'ForecastCategoryName'
  AND ofh.newvalue = 'Commit'
  AND o.e_purchase_date__c IS NULL
  GROUP BY 1
), opportunity_paid_netsuite AS (
    SELECT 
      netsuite_financial.netsuite_conn__opportunity__c AS opportunity_sfid,
      netsuite_conn__type__c AS paid_type,
      netsuite_conn__transaction_date__c AS paid_date,
      TRUE AS paid
    FROM {{ source('orgm', 'netsuite_conn__netsuite_financial__c') }} AS netsuite_financial
    WHERE (netsuite_conn__type__c = 'Cash Sale' AND netsuite_conn__status__c = 'Deposited') OR (netsuite_conn__type__c = 'Customer Payment' AND netsuite_conn__status__c IN ('Deposited','Not Deposited'))
    GROUP BY 1, 2, 3, 4
), opportunity_marketing AS (
    SELECT 
      opportunity.sfid AS opportunity_sfid,
      BOOLOR_AGG(TRUE) AS marketing_generated
    FROM {{ source('orgm', 'opportunity') }}
    LEFT JOIN {{ source('orgm', 'opportunitycontactrole') }} ON opportunity.sfid = opportunitycontactrole.opportunityid
    LEFT JOIN {{ source('orgm', 'contact') }}  AS contact ON opportunitycontactrole.contactid = contact.sfid
    WHERE contact.first_mql_date__c < opportunity.createddate
      AND opportunity.type  IN ('Account Expansion', 'New Subscription')
      AND contact.first_mql_date__c IS NOT NULL
    GROUP BY 1
), opportunity_fc_amounts AS (
    SELECT 
        opportunity.sfid as opportunity_sfid,
        SUM(CASE WHEN forecastcategoryname = 'Commit' THEN amount ELSE 0 END) AS amount_in_commit,
        SUM(CASE WHEN forecastcategoryname = 'Best Case' THEN amount ELSE 0 END) AS amount_in_best_case,
        SUM(CASE WHEN forecastcategoryname = 'Pipeline' THEN amount ELSE 0 END) AS amount_in_pipeline
    FROM {{ source('orgm','opportunity') }}
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
      first_commit_date,
      last_commit_date,
      num_times_slipped,
      marketing_generated,
      amount_in_commit,
      amount_in_best_case,
      amount_in_pipeline,
      COALESCE(paid, FALSE) AS paid,
      paid_type,
      paid_date,
      SUM(new_amount__c) AS sum_new_amount,
      SUM(expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) AS sum_expansion_amount,
      SUM(expansion_amount__c + 365 * (coterm_expansion_amount__c + leftover_expansion_amount__c)/NULLIF((end_date__c::date - start_date__c::date + 1),0)) AS sum_expansion_w_proration_amount,
      SUM(renewal_amount__c) AS sum_renewal_amount,
      SUM(multi_amount__c) AS sum_multi_amount,
      SUM(ren_multi_amount__c) AS sum_ren_multi_amount
  FROM {{ source('orgm', 'opportunity') }}
  LEFT JOIN {{ source('orgm', 'opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
  LEFT JOIN w_end_date ON opportunity.sfid = w_end_date.opportunity_sfid
  LEFT JOIN w_start_date ON opportunity.sfid = w_start_date.opportunity_sfid
  LEFT JOIN w_oppt_commit ON opportunity.sfid = w_oppt_commit.opportunity_sfid
  LEFT JOIN opportunity_paid_netsuite ON opportunity.sfid = opportunity_paid_netsuite.opportunity_sfid
  LEFT JOIN opportunity_marketing ON opportunity.sfid = opportunity_marketing.opportunity_sfid
  LEFT JOIN opportunity_fc_amounts ON opportunity.sfid = opportunity_fc_amounts.opportunity_sfid
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18
)

 SELECT * FROM opportunity_ext
