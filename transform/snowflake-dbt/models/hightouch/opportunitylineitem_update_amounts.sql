{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with opportunitylineitem_totals as (
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
), opportunitylineitem_update_amounts as (
  select oli_totals.* 
  from {{ ref('opportunitylineitem') }}
  join opportunitylineitem_totals as oli_totals on oli_totals.sfid = opportunitylineitem.sfid
  where opportunitylineitem.product_line_type__c != 'Self-Service' and not opportunitylineitem.amount_manual_override__c 
)

select opportunitylineitem_update_amounts.* 
from opportunitylineitem_update_amounts
join {{ ref('opportunitylineitem') }} as oli on oli.sfid = opportunitylineitem_update_amounts.sfid
where
  (round(oli.new_amount__c,2), 
  round(oli.renewal_amount__c,2), 
  round(oli.expansion_amount__c,2), 
  round(oli.coterm_expansion_amount__c,2), 
  round(oli.leftover_expansion_amount__c,2), 
  round(oli.multi_amount__c,2), 
  round(oli.renewal_multi_amount__c,2))
  is distinct from
  (round(opportunitylineitem_update_amounts.total_new_amount,2),
  round(opportunitylineitem_update_amounts.total_renewal_amount,2),
  round(opportunitylineitem_update_amounts.total_expansion_amount,2),
  round(opportunitylineitem_update_amounts.total_coterm_amount,2),
  round(opportunitylineitem_update_amounts.total_leftover_amount,2),
  round(opportunitylineitem_update_amounts.total_multi_amount,2),
  round(opportunitylineitem_update_amounts.total_ren_multi_amount,2))
