{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

with a as (
    select
        bin_avg_arr
        ,cohort_fiscal_quarter
        ,fiscal_quarter_no
        ,sum(arr_delta) delta_arr
        ,sum(cnt_change) delta_cnt
    from {{ ref( 'arr_rollforward') }}
    --from analytics.finance.arr_rollforward
        where cohort_month >= date '2017-01-31'
        and industry != 'Non-Profit'
    group by 1,2,3
    order by 1,2,3
)

--CALCULATE PREVIOUS NUMBERS WITH WINDOW FUNCTIONS
,b as (
select
    bin_avg_arr
    ,cohort_fiscal_quarter
    ,fiscal_quarter_no
    ,sum(delta_arr) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no rows between unbounded preceding and 1 preceding) as prev_cum_arr
    ,sum(delta_arr) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no) as cum_arr
    ,sum(delta_cnt) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no rows between unbounded preceding and 1 preceding) as prev_cust_cnt    
    ,sum(delta_cnt) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no) as cust_cnt
    ,round(div0(cum_arr,cust_cnt),0) as avg_arr
from a)

--COMPILE WINDOW CALCULATIONS
,d as (
select
    *
    ,first_value(cum_arr) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no) as first_cum_arr
    ,round(div0(cum_arr,first_cum_arr),2) as net_dollar_retention
    ,first_value(avg_arr) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no) as first_arr_avg
    ,first_value(cust_cnt) over (partition by bin_avg_arr||cohort_fiscal_quarter order by fiscal_quarter_no) as first_cnt
    ,round(div0(avg_arr,first_arr_avg),2) as upsell_downsell
    ,round(div0(cust_cnt,first_cnt),2) as retained
    ,round(div0(cust_cnt,prev_cust_cnt),2)-1 as churn_qoq
from b
order by cohort_fiscal_quarter, fiscal_quarter_no
)

--REPORT LTV
select 
    bin_avg_arr
    ,fiscal_quarter_no
    ,fiscal_quarter_no/4 as fiscal_year
    ,round(avg(avg_arr),0) as arr_avg
    ,round(div0(arr_avg,first_value(arr_avg) over (partition by bin_avg_arr order by fiscal_quarter_no)),2) as net_expansion
    ,round(avg(retained),6) as retention
    ,round(retention * arr_avg,0) as expected_cash
    ,0.97 as net_margin
    ,round(expected_cash * net_margin,0) as net_cash
    ,sum(net_cash) over (partition by bin_avg_arr order by fiscal_quarter_no asc) as ltv
from d
where fiscal_quarter_no in (0,4,8,12,16,20,24)
group by 1,2
order by 1,2 asc

