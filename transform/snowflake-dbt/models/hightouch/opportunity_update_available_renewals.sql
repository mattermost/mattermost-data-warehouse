{{config({
    "materialized": 'table',
    "schema": "hightouch"
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
  where 
    (opportunity.available_renewal__c) 
    is distinct from 
    (available_renewal)
)

select * from opportunity_update_available_renewals