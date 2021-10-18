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
  FROM {{ ref('opportunitylineitem') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_start_date AS ( 
  SELECT
        opportunityid as opportunity_sfid,
        min(start_date__c) as min_start_date,
        max(start_date__c) as max_start_date,
        count(distinct start_date__c) as num_diff_start_dates
  FROM {{ ref('opportunitylineitem') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_oppt_commit AS ( 
  SELECT
       o.sfid as opportunity_sfid,
       min(ofh.createddate) as first_commit_date,
       max(ofh.createddate) as last_commit_date,
       count(distinct(to_char(ofh.createddate,'YYYY-MM'))) as num_times_slipped
  FROM {{ ref('opportunityfieldhistory') }} ofh
  LEFT JOIN {{ ref('opportunity') }} o ON (ofh.opportunityid = o.sfid)
  WHERE ofh.field = 'ForecastCategoryName'
  AND ofh.newvalue = 'Commit'
  AND o.e_purchase_date__c IS NULL
  GROUP BY 1
), opportunity_paid_netsuite AS (
  SELECT
    netsuite_financial.name,
    max(netsuite_financial.netsuite_conn__opportunity__c) AS opportunity_sfid,
    max(netsuite_conn__type__c) AS paid_type,
    max(netsuite_conn__transaction_date__c) AS paid_date,
    max(payment_method__c) AS payment_method,
    TRUE AS paid
  FROM {{ ref('netsuite_conn__netsuite_financial__c') }} AS netsuite_financial
  WHERE (netsuite_conn__type__c = 'Cash Sale' AND netsuite_conn__status__c = 'Deposited') OR (netsuite_conn__type__c = 'Customer Payment' AND netsuite_conn__status__c IN ('Deposited','Not Deposited'))
  GROUP BY 1,6
), opportunity_marketing AS (
  SELECT 
    opportunity.sfid AS opportunity_sfid,
    BOOLOR_AGG(TRUE) AS marketing_generated
  FROM {{ ref('opportunity') }}
  LEFT JOIN {{ ref('opportunitycontactrole') }} ON opportunity.sfid = opportunitycontactrole.opportunityid
  LEFT JOIN {{ ref('contact') }} AS contact ON opportunitycontactrole.contactid = contact.sfid
  LEFT JOIN {{ ref('lead') }} AS lead ON lead.convertedcontactid = contact.sfid
  WHERE least(coalesce(contact.first_mql_date__c,current_timestamp()), coalesce(lead.first_mql_date__c,current_timestamp())) < opportunity.createddate
    AND opportunity.type  IN ('Account Expansion', 'New Subscription')
    AND coalesce(contact.first_mql_date__c, lead.first_mql_date__c) IS NOT NULL
  GROUP BY 1
), opportunity_fc_amounts AS (
  SELECT 
      opportunity.sfid as opportunity_sfid,
      SUM(CASE WHEN forecastcategoryname = 'Commit' THEN amount ELSE 0 END) AS amount_in_commit,
      SUM(CASE WHEN forecastcategoryname = 'Best Case' THEN amount ELSE 0 END) AS amount_in_best_case,
      SUM(CASE WHEN forecastcategoryname = 'Pipeline' THEN amount ELSE 0 END) AS amount_in_pipeline
  FROM {{ ref('opportunity') }}
  GROUP BY 1
), opp_products AS (
  SELECT 
      opportunity.sfid as id,
      SUM(
          IFF(PRODUCT_LINE_TYPE__C = 'Expansion', TOTALPRICE/((END_DATE__C::date - START_DATE__C::date +1)/365),
          IFF(PRODUCT_LINE_TYPE__C = 'New', TOTALPRICE/((END_DATE__C::date - START_DATE__C::date + 1)/365), 0))
          ) as Net_New_ARR__c
  FROM {{ ref('opportunity') }}
  LEFT JOIN {{ ref('opportunitylineitem') }} on opportunity.sfid = opportunitylineitem.opportunityid and (END_DATE__C::date - START_DATE__C::date + 1) != 0
  GROUP BY 1
), opp_net_new_arr_override AS (
  SELECT
      opportunity.sfid as id,
      IFNULL(opportunity.Override_Total_Net_New_ARR__c, Net_New_ARR__c) as Net_New_ARR_with_Override__c
  FROM {{ ref('opportunity') }}
  LEFT JOIN opp_products on opp_products.id = opportunity.sfid
), opp_created_by_segment as (
  SELECT
    opportunity.sfid as opportunity_sfid,
    case 
      when userrole.name like '%Federal%' then 'Federal'
      when coalesce(numberofemployees,0) <= 999 and userrole.name = 'Sales Dev Reps' then 'Commercial BDR'
      when coalesce(numberofemployees,0) <= 999 and userrole.name not like '%Federal%' then 'Commercial AE'
      when coalesce(numberofemployees,0) > 999 and userrole.name = 'Sales Dev Reps' then 'Enterprise BDR'
      when coalesce(numberofemployees,0) > 999 and userrole.name not like '%Federal%' then 'Enterprise AE'
    else null 
    end as created_by_segment
  FROM {{ ref('opportunity') }}
  JOIN {{ ref('user') }} as creator on opportunity.ownerid = creator.sfid
  JOIN {{ ref('userrole') }} on creator.userroleid = userrole.sfid
  JOIN {{ ref('account') }} on opportunity.accountid = account.sfid
), opp_owner_role AS (
  SELECT 
    opportunity.sfid as opportunity_sfid,
    userrole.sfid as owner_role_sfid,
    userrole.name as owner_role_name
  FROM {{ ref('opportunity') }}
  JOIN {{ ref('user') }} as owner on opportunity.ownerid = owner.sfid
  JOIN {{ ref('userrole') }} on owner.userroleid = userrole.sfid
), opportunity_ext AS (
  SELECT
      opportunity.sfid as opportunity_sfid,
      opportunity.accountid as accountid,
      case when owner_role_name like '%Federal%' then 'Federal' when coalesce(numberofemployees,0) <= 999 then 'Commercial' when coalesce(numberofemployees,0) > 999 then 'Enterprise' end as market_segment,
      opp_owner_role.owner_role_name,
      opp_created_by_segment.created_by_segment,
      opportunity.closedate,
      case when iswon then datediff('day',opportunity.createddate, opportunity.closedate) else datediff('day',opportunity.createddate, current_date()) end as age,
      iswon,
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
      payment_method,
      Net_New_ARR__c,
      Net_New_ARR_with_Override__c,
      SUM(new_amount__c) AS sum_new_amount,
      SUM(expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) AS sum_expansion_amount,
      SUM(expansion_amount__c + 365 * (coterm_expansion_amount__c + leftover_expansion_amount__c)/NULLIF((end_date__c::date - start_date__c::date + 1),0)) AS sum_expansion_w_proration_amount,
      SUM(renewal_amount__c) AS sum_renewal_amount,
      SUM(multi_amount__c) AS sum_multi_amount,
      SUM(renewal_multi_amount__c) AS sum_renewal_multi_amount
  FROM {{ ref('opportunity') }}
  LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
  JOIN {{ ref('account') }} on opportunity.accountid = account.sfid
  LEFT JOIN w_end_date ON opportunity.sfid = w_end_date.opportunity_sfid
  LEFT JOIN w_start_date ON opportunity.sfid = w_start_date.opportunity_sfid
  LEFT JOIN w_oppt_commit ON opportunity.sfid = w_oppt_commit.opportunity_sfid
  LEFT JOIN opportunity_paid_netsuite ON opportunity.sfid = opportunity_paid_netsuite.opportunity_sfid
  LEFT JOIN opportunity_marketing ON opportunity.sfid = opportunity_marketing.opportunity_sfid
  LEFT JOIN opportunity_fc_amounts ON opportunity.sfid = opportunity_fc_amounts.opportunity_sfid
  LEFT JOIN opp_products ON opportunity.sfid = opp_products.id
  LEFT JOIN opp_net_new_arr_override on opportunity.sfid = opp_net_new_arr_override.id
  LEFT JOIN opp_created_by_segment on opportunity.sfid = opp_created_by_segment.opportunity_sfid
  LEFT JOIN opp_owner_role on opportunity.sfid = opp_owner_role.opportunity_sfid
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27
)

 SELECT * FROM opportunity_ext
