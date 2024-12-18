WITH w_end_date AS (
  SELECT
        opportunity_id,
        min(end_date__c) as min_end_date,
        max(end_date__c) as max_end_date,
        count(distinct end_date__c) as num_diff_end_dates
  FROM {{ ref('stg_salesforce__opportunity_line_item') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_start_date AS ( 
  SELECT
        opportunity_id,
        min(start_date__c) as min_start_date,
        max(start_date__c) as max_start_date,
        count(distinct start_date__c) as num_diff_start_dates
  FROM {{ ref('stg_salesforce__opportunity_line_item') }}
  WHERE product_type__c='Recurring'
  GROUP BY 1
), w_oppt_commit AS ( 
  SELECT
       o.opportunity_id,
       min(ofh.createddate) as first_commit_date,
       max(ofh.createddate) as last_commit_date,
       count(distinct(to_char(ofh.created_date,'YYYY-MM'))) as num_times_slipped
  FROM {{ ref('stg_salesforce__opportunity_field_history_migrated') }} ofh
  LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON (ofh.opportunity_id = o.opportunity_id)
  WHERE ofh.field = 'ForecastCategoryName'
  AND ofh.newvalue = 'Commit'
  AND o.e_purchase_date__c IS NULL
  GROUP BY 1
), opportunity_paid_netsuite AS (
  SELECT
    netsuite_financial.name,
    max(netsuite_financial.netsuite_conn__opportunity__c) AS opportunity_id,
    max(netsuite_conn__type__c) AS paid_type,
    max(netsuite_conn__transaction_date__c) AS paid_date,
    max(payment_method__c) AS payment_method,
    TRUE AS paid
  FROM {{ ref('stg_salesforce__netsuite_financial__c') }} AS netsuite_financial
  WHERE (netsuite_conn__type__c = 'Cash Sale' AND netsuite_conn__status__c = 'Deposited') OR (netsuite_conn__type__c = 'Customer Payment' AND netsuite_conn__status__c IN ('Deposited','Not Deposited'))
  GROUP BY 1,6
), opportunity_marketing AS (
  SELECT 
    opportunity.opportunity_id AS opportunity_id,
    BOOLOR_AGG(TRUE) AS marketing_generated
  FROM {{ ref('stg_salesforce__opportunity') }} AS opportunity
  LEFT JOIN {{ ref('stg_salesforce__opportunity_contact_role') }} AS opportunitycontactrole ON opportunity.opportunity_id = opportunitycontactrole.opportunity_id
  LEFT JOIN {{ ref('stg_salesforce__contact') }} AS contact ON opportunitycontactrole.contact_id = contact.contact_id
  LEFT JOIN {{ ref('stg_salesforce__lead') }} AS lead ON lead.converted_contact_id = contact.contact_id
  WHERE least(coalesce(contact.first_mql_date__c,current_timestamp()), coalesce(lead.first_mql_date__c,current_timestamp())) < opportunity.created_at
    AND opportunity.type  IN ('Account Expansion', 'New Subscription')
    AND coalesce(contact.first_mql_date__c, lead.first_mql_date__c) IS NOT NULL
  GROUP BY 1
), opportunity_fc_amounts AS (
  SELECT 
      opportunity_id,
      SUM(CASE WHEN forecast_category_name = 'Commit' THEN amount ELSE 0 END) AS amount_in_commit,
      SUM(CASE WHEN forecast_category_name = 'Best Case' THEN amount ELSE 0 END) AS amount_in_best_case,
      SUM(CASE WHEN forecast_category_name = 'Pipeline' THEN amount ELSE 0 END) AS amount_in_pipeline
  FROM {{ ref('stg_salesforce__opportunity') }}
  GROUP BY 1
), opp_products AS (
  SELECT 
      opportunity.opportunity_id,
      SUM(
          IFF(PRODUCT_LINE_TYPE__C = 'Expansion', TOTAL_PRICE/((END_DATE__C::date - START_DATE__C::date +1)/365),
          IFF(PRODUCT_LINE_TYPE__C = 'New', TOTAL_PRICE/((END_DATE__C::date - START_DATE__C::date + 1)/365), 0))
          ) as Net_New_ARR__c
  FROM {{ ref('stg_salesforce__opportunity') }} opportunity
  LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} oli on opportunity.opportunity_id = oli.opportunity_id and (END_DATE__C::date - START_DATE__C::date + 1) != 0
  GROUP BY 1
), opp_net_new_arr_override AS (
  SELECT
      opportunity.opportunity_id,
      IFNULL(opportunity.Override_Total_Net_New_ARR__c, Net_New_ARR__c) as Net_New_ARR_with_Override__c
  FROM {{ ref('stg_salesforce__opportunity') }} opportunity
  LEFT JOIN opp_products on opp_products.opportunity_id = opportunity.opportunity_id
), opp_created_by_segment as (
  SELECT
    opportunity.opportunity_id,
    case 
      when userrole.name like '%Federal%' then 'Federal'
      when coalesce(number_of_employees,0) <= 999 and userrole.name = 'Sales Dev Reps' then 'Commercial BDR'
      when coalesce(number_of_employees,0) <= 999 and userrole.name not like '%Federal%' then 'Commercial AE'
      when coalesce(number_of_employees,0) > 999 and userrole.name = 'Sales Dev Reps' then 'Enterprise BDR'
      when coalesce(number_of_employees,0) > 999 and userrole.name not like '%Federal%' then 'Enterprise AE'
    else null 
    end as created_by_segment
  FROM {{ ref('stg_salesforce__opportunity') }} opportunity
  JOIN {{ ref('stg_salesforce__user') }} creator on opportunity.owner_id = creator.user_id
  JOIN {{ ref('stg_salesforce__user_role') }} userrole on creator.user_role_id = userrole.user_role_id
  JOIN {{ ref('stg_salesforce__account') }} account on opportunity.account_id = account.account_id
), opp_owner_role AS (
  SELECT 
    opportunity.opportunity_id,
    userrole.user_role_id,
    userrole.name as owner_role_name
  FROM {{ ref('stg_salesforce__opportunity') }} as opportunity
  JOIN {{ ref('stg_salesforce__user') }} as owner on opportunity.owner_id = owner.user_id
  JOIN {{ ref('stg_salesforce__user_role') }} as userrole on owner.user_role_id = userrole.user_role_id
), opportunity_ext AS (
  SELECT
      opportunity.opportunity_id,
      opportunity.account_id as account_id,
      case when owner_role_name like '%Federal%' then 'Federal' when coalesce(number_of_employees,0) <= 999 then 'Commercial' when coalesce(number_of_employees,0) > 999 then 'Enterprise' end as market_segment,
      opp_owner_role.owner_role_name,
      opp_created_by_segment.created_by_segment,
      opportunity.close_at as close_date,
      case when is_won then datediff('day',opportunity.created_at, opportunity.close_at) else datediff('day',opportunity.created_at, current_date()) end as age,
      is_won,
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
  FROM {{ ref('stg_salesforce__opportunity') }} opportunity
  LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} opportunitylineitem ON opportunity.opportunity_id = opportunitylineitem.opportunity_id
  JOIN {{ ref('stg_salesforce__account') }} account on opportunity.account_id = account.account_id
  LEFT JOIN w_end_date ON opportunity.opportunity_id = w_end_date.opportunity_id
  LEFT JOIN w_start_date ON opportunity.opportunity_id = w_start_date.opportunity_id
  LEFT JOIN w_oppt_commit ON opportunity.opportunity_id = w_oppt_commit.opportunity_id
  LEFT JOIN opportunity_paid_netsuite ON opportunity.opportunity_id = opportunity_paid_netsuite.opportunity_id
  LEFT JOIN opportunity_marketing ON opportunity.opportunity_id = opportunity_marketing.opportunity_id
  LEFT JOIN opportunity_fc_amounts ON opportunity.opportunity_id = opportunity_fc_amounts.opportunity_id
  LEFT JOIN opp_products ON opportunity.opportunity_id = opp_products.opportunity_id
  LEFT JOIN opp_net_new_arr_override on opportunity.opportunity_id = opp_net_new_arr_override.opportunity_id
  LEFT JOIN opp_created_by_segment on opportunity.opportunity_id = opp_created_by_segment.opportunity_id
  LEFT JOIN opp_owner_role on opportunity.opportunity_id = opp_owner_role.opportunity_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27
)

 SELECT * FROM opportunity_ext
