{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--this query shows the arr balance trend by month and which fiscal year cohort contributes to it
--sum active arr and sum active cnt
--also enables the calculation of ltv
--derived table of arr rollforward

with a as (
  select
        last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,cohort)))))) as fiscal_year
        ,report_month
        ,sum(tcv) as tcv_signed
        ,sum(arr) as arr_signed
        ,sum(expire) as arr_expired
        ,sum(arr_delta) as arr_change
        ,sum(new_arr) as new
        ,sum(resurrect_arr) as resurrected
        ,sum(total_expansion) as expanded
        ,sum(contraction) as contracted
        ,sum(churn) as churned
        ,sum(cnt_change) as count_change
        ,sum(cnt_new_account) as new_cnt
        ,sum(cnt_resurrect) as resurrect_cnt
        ,sum(cnt_churn) as churn_cnt
        ,sum(cnt_total_expand) as expansion_cnt
        ,sum(cnt_contraction) as contraction_cnt
    --from analytics.finance.arr_rollforward
    from {{ ref( 'arr_rollforward') }}
    group by 1,2
)


select
      fiscal_year
      ,report_month 
      ,tcv_signed
      ,arr_signed
      ,arr_expired
      ,arr_change
      ,new
      ,resurrected
      ,expanded
      ,contracted
      ,churned
      ,sum(arr_change) over (partition by fiscal_year order by report_month) as active_arr
      ,new_cnt
      ,resurrect_cnt
      ,churn_cnt
      ,count_change as change_cnt
      ,sum(count_change) over (partition by fiscal_year order by report_month) as active_cnt
      ,expansion_cnt
      ,contraction_cnt
  from a
  order by fiscal_year desc, report_month asc