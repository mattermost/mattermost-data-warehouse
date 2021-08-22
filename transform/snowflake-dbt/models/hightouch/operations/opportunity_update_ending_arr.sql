{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with opportunity_end_date as (
  select distinct
    opportunity_sfid,
    opportunity.renewed_by_opportunity_id__c as renewal_opportunity_sfid,
    max_end_date
  from {{ ref('opportunity_ext') }}
  join {{ ref('opportunity') }} on opportunity_ext.opportunity_sfid = opportunity.sfid
  where opportunity.iswon 
),

opportunity_ending_arr as (
  select
    opportunity_end_date.opportunity_sfid,
    opportunity_end_date.renewal_opportunity_sfid,
    coalesce(won_arr,0) as ending_arr
  from opportunity_end_date
  join {{ ref('opportunity_daily_arr') }} 
    on opportunity_end_date.max_end_date = opportunity_daily_arr.day
      and opportunity_end_date.opportunity_sfid = opportunity_daily_arr.opportunity_sfid
),

opportunity_update_ending_arr as (
  select
    opportunity_ending_arr.opportunity_sfid,
    coalesce(opportunity_ending_arr.ending_arr,0) as ending_arr,
    sum(coalesce(original_opportunity_ending_arr.ending_arr,0)) as original_opportunity_ending_arr
  from opportunity_ending_arr
  left join opportunity_ending_arr as original_opportunity_ending_arr
    on opportunity_ending_arr.opportunity_sfid = original_opportunity_ending_arr.renewal_opportunity_sfid
  group by 1, 2
)

select * from opportunity_update_ending_arr