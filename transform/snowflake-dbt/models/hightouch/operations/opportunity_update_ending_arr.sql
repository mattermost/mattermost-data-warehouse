{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with opportunity_end_date as (
  select distinct
    opportunity_sfid,
    max_end_date
  from {{ ref('opportunity_ext') }}
  where iswon 
),

opportunity_update_ending_arr as (
  select
    opportunity_end_date.opportunity_sfid,
    coalesce(won_arr,0) as ending_arr
  from opportunity_end_date
  join {{ ref('opportunity_daily_arr') }} 
    on opportunity_end_date.max_end_date = opportunity_daily_arr.day
      and opportunity_end_date.opportunity_sfid = opportunity_daily_arr.opportunity_sfid
)

select * from opportunity_update_ending_arr