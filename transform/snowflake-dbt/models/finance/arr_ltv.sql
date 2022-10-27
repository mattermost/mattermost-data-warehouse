{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--QUERY FOR CALCULATING LTV BASED ON CONTRIBUTION MARGIN OF 0.97 AND 
--THIS IS THE SUBQUERY FOR BIN ARR
with a as (
  select
        bin_avg_arr,
        cohort_yr,
        report_mo,
        datediff('month',cohort_yr,report_mo) as month_after,
        sum(arr_os) as end_arr,
        sum(active_cnt) as end_customers,
        round(div0(end_arr,end_customers),0) as avg_arr
  from {{ref('arr_vintages')}}
  --from analytics.finance.arr_vintages
  where cohort_yr > date '2017-01-31'
  group by 1,2,3,4
  order by 1,2,3,4
)
,b as (
  select
        bin_avg_arr,
        cohort_yr,
        end_arr as max_arr,
        avg_arr as start_avg_arr,
        end_customers as max_customers
  from a
  where month_after = 0
)
--THIS IS THE SUBQUERY FOR ALL BINS
,c as (
  select
      '0 - ALL' as bin_avg_arr,
      cohort_yr,
      report_mo,
      datediff('month',cohort_yr,report_mo) as month_after,
      sum(arr_os) as end_arr,
      sum(active_cnt) as end_customers,
      round(div0(end_arr,end_customers),0) as avg_arr
from {{ref('arr_vintages')}}
--from analytics.finance.arr_vintages
where cohort_yr > date '2017-01-31'
group by 1,2,3,4
order by 1,2,3,4
)

,d as (
  select
        bin_avg_arr,
        cohort_yr,
        end_arr as max_arr,
        avg_arr as start_avg_arr,
        end_customers as max_customers
  from c
  where month_after = 0
)

--CONSOLIDATING REPORT
,consol as (

select
        a.bin_avg_arr,
        a.cohort_yr,
        a.month_after,
        a.end_arr,
        round(div0(a.avg_arr,b.start_avg_arr),2) as expand_rate,
        round(div0(a.end_arr,max_arr),2) as ndr_pcnt,
        a.end_customers,
        round(div0(a.end_customers,max_customers),2) as cust_retention,
        a.avg_arr,
        round(div0(a.end_arr,a.end_customers)*cust_retention,0) as retained_ltv_arr,
        round(retained_ltv_arr * 0.97,0) as cmargin_ltv_cashflow,
        sum(cmargin_ltv_cashflow) over (partition by a.bin_avg_arr||a.cohort_yr order by a.month_after) as cum_ltv
from a
left join b on b.cohort_yr = a.cohort_yr and a.bin_avg_arr = b.bin_avg_arr
where month_after in (0,12,24,36,48,60,72,84)

union

select
        c.bin_avg_arr,
        c.cohort_yr,
        c.month_after,
        c.end_arr,
        round(div0(c.avg_arr,d.start_avg_arr),2) as expand_rate,
        round(div0(c.end_arr,d.max_arr),2) as ndr_pcnt,
        c.end_customers,
        round(div0(c.end_customers,max_customers),2) as cust_retention,
        c.avg_arr,
        round(div0(c.end_arr,c.end_customers)*cust_retention,2) as retained_ltv_arr,
        round(retained_ltv_arr * 0.97,2) as cmargin_ltv_cashflow,
        sum(cmargin_ltv_cashflow) over (partition by c.bin_avg_arr||c.cohort_yr order by c.month_after) as cum_ltv
from c
left join d on d.cohort_yr = c.cohort_yr and d.bin_avg_arr = c.bin_avg_arr
where month_after in (0,12,24,36,48,60,72,84)
order by bin_avg_arr, cohort_yr, month_after
)

--ADDING UNIQUE KEY AND YEAR END MARKERS
select
        *,
        bin_avg_arr||'-'||cohort_yr||'-'||month_after as unique_key,
        dateadd('month',month_after,cohort_yr) as fiscal_qtr
from consol
 
       

