{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

with a as (
    select
          --partition variables  remember to include in joins
          cohort_fiscal_yr as cohort_yr,
          cohort_fiscal_qtr,
          bin_avg_arr,
          account_name,
          account_id,
          cohort_month,
          --distinct partition ordering
          report_mo,
          --fields not important for ordering because of 1 to 1 correspondence with report_mo
          fiscal_year_no,
          fiscal_yr,
          sum(new) as new_arr,
          sum(late_renewal) as late_renewal_arr,
          sum(resurrected) as resurrect_arr,
          sum(expanded) as expand_arr,
          sum(contracted) as contract_arr,
          sum(churned) as churn_arr,
          sum(arr_delta) as arr_change,
          sum(cnt_new) as new_cnt,
          sum(cnt_late_renewal) as late_renewal_cnt,
          sum(cnt_resurrected) as resurrect_cnt,
          sum(cnt_churned) as churned_cnt,
          sum(cnt_changed) as delta_cnt,
          sum(cnt_expanded) as expand_cnt,
          sum(cnt_contracted) as contract_cnt
    --from analytics.finance.arr_reporting
    from {{ ref( 'arr_reporting') }}
          group by 1,2,3,4,5,6,7,8,9
)

,b as (
    select 
        distinct report_mo,fiscal_yr
    from a 
)

,b1 as (
    select 
        distinct cohort_yr,bin_avg_arr, account_name, account_id ,cohort_month
    from a
)

,b3 as (
    select
        *
    from b
        left join b1 where report_mo >= cohort_month
    order by 1,2
)
--distinct demographic data to the account_id
,d as (
    select 
        account_id, geo, company_type, company_size, industry,cohort_month, max(product) as product
    --from arr_reporting  
    from {{ ref( 'arr_reporting') }}
        group by 1,2,3,4,5,6
)

,c as (
    select
        b3.report_mo,
        b3.bin_avg_arr,
        b3.cohort_yr,
        b3.cohort_month,
        b3.account_name,
        b3.account_id,
        iff(date_part('month',b3.report_mo) in (4,7,10,1),1,0) as quarter_end,
        coalesce(a.fiscal_yr,b3.fiscal_yr) as fiscalyr,
        coalesce(a.fiscal_year_no,iff(b3.cohort_yr<fiscalyr,datediff('year',b3.cohort_yr,fiscalyr),0)) as fiscal_yearno,
        coalesce(a.new_arr,0) as new,
        coalesce(a.late_renewal_arr,0) as late_renew,
        coalesce(a.resurrect_arr,0) as resurrect,
        coalesce(a.expand_arr,0) as expand,
        coalesce(a.contract_arr,0) as contract,
        coalesce(a.churn_arr,0) as churn,
        coalesce(a.arr_change,0) as arr_delta,
        coalesce(a.new_cnt,0) as cnt_new,
        coalesce(a.late_renewal_cnt,0) as cnt_late_renewal,
        coalesce(a.resurrect_cnt,0) as cnt_resurrect,
        coalesce(a.churned_cnt,0) as cnt_churned,
        coalesce(a.delta_cnt,0) as cnt_delta,
        coalesce(a.expand_cnt,0) as cnt_expand,
        coalesce(a.contract_cnt,0) as cnt_contract
    from b3
        left join a on a.report_mo = b3.report_mo and a.bin_avg_arr = b3.bin_avg_arr and a.cohort_yr = b3.cohort_yr and a.account_id = b3.account_id
)

,output as (
    select
        c.*,
        sum(arr_delta) over (partition by cohort_yr||account_id order by report_mo) as arr_os,
        sum(cnt_delta) over (partition by cohort_yr||account_id order by report_mo) as active_cnt
    from c    
)

select
    output.*,
    first_value (output.arr_os) over (partition by output.account_id order by output.report_mo) as first_arr,
    first_value (output.active_cnt) over (partition by output.account_id order by output.report_mo) as first_cnt,
    case 
        when date_part('month',output.cohort_month) = 1 then 12
        else date_part('month',output.cohort_month)::number -1
        end as orderbymonth,
    datediff('month',output.cohort_month,output.report_mo) as month_since_purchase,
    iff(month_since_purchase = 0,output.active_cnt,0) as initial_cohort_size,
    datediff('quarter',output.cohort_month,report_mo) as cohort_qtr_no,
    d.geo,
    d.company_type,
    d.company_size,
    d.industry,
    d.product,
    iff(c.customer_type is null, 'regular',c.customer_type) as cust_type
from output
    left join d on d.account_id = output.account_id
    left join analytics.finance.arr_customertype c on c.account_id = output.account_id
    order by cohort_month,account_id, report_mo