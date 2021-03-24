{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with opportunitylineitem_update_amounts as (
  select
    opportunitylineitem.sfid as sfid,
    case when opportunitylineitem.product_line_type__c = 'New' THEN opportunitylineitem.totalprice ELSE 0 END AS total_new_amount,
    case when opportunitylineitem.product_line_type__c = 'Ren' THEN opportunitylineitem.totalprice ELSE 0 END AS total_renewal_amount,
    case when opportunitylineitem.product_line_type__c = 'Expansion' AND opportunitylineitem.is_prorated_expansion__c IS NULL THEN opportunitylineitem.totalprice ELSE 0 END AS total_expansion_amount,
    case when opportunitylineitem.product_line_type__c = 'Expansion' AND opportunitylineitem.is_prorated_expansion__c = 'Co-Termed Expansion' THEN opportunitylineitem.totalprice ELSE 0 END AS total_coterm_amount,
    case when opportunitylineitem.product_line_type__c = 'Expansion' AND opportunitylineitem.is_prorated_expansion__c = 'Leftover Expansion' THEN opportunitylineitem.totalprice ELSE 0 END AS total_leftover_amount,
    case when opportunitylineitem.product_line_type__c = 'Multi' THEN opportunitylineitem.totalprice ELSE 0 END AS total_multi_amount,
    case when opportunitylineitem.product_line_type__c = 'Ren-Multi' THEN opportunitylineitem.totalprice ELSE 0 END AS total_ren_multi_amount
  from {{ ref('opportunitylineitem') }}
), opportunitylineitem_updates as (
  select oli_totals.* 
  from {{ ref('opportunitylineitem') }} as oli
  join opportunitylineitem_totals as oli_totals on oli_totals.sfid = oli.sfid
  where oli.product_line_type__c != 'Self-Service' and not oli.amount_manual_override__c and 
    (oli.new_amount__c, oli.renewal_amount__c, oli.expansion_amount__c, oli.coterm_expansion_amount__c, oli.leftover_expansion_amount__c, oli.multi_amount__c, oli.renewal_multi_amount__c) 
    is distinct from
    (oli_totals.total_new_amount, oli_totals.total_renewal_amount, oli_totals.total_expansion_amount, oli_totals.total_coterm_amount, oli_totals.total_leftover_amount, oli_totals.total_multi_amount, oli_totals.total_ren_multi_amount)
)

select * from opportunitylineitem_update_amounts