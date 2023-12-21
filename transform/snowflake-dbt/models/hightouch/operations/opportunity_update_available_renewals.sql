{{config({
    "materialized": 'table',
    "schema": "hightouch",
    "tags":["deprecated"]
  })
}}

with available_renewals as (
  select renewal_opportunity, available_renewal as avail_ren
  from {{ source('sales_and_cs_gsheets','available_renewals_current_fy') }}
  union all 
  select renewal_opportunity, available_renewal as avail_ren
  from {{ source('sales_and_cs_gsheets','available_renewals_historical') }}
), opportunity_update_available_renewals as (
  select opportunity.sfid, case when available_renewals.renewal_opportunity is null then null else available_renewals.avail_ren end as available_renewal
  from {{ ref('opportunity') }}
  left join available_renewals on available_renewals.renewal_opportunity = opportunity.sfid
)

select opportunity_update_available_renewals.* 
from opportunity_update_available_renewals
join {{ ref('opportunity') }} on opportunity.sfid = opportunity_update_available_renewals.sfid
where 
  (opportunity.available_renewal__c) 
  is distinct from 
  (opportunity_update_available_renewals.available_renewal)