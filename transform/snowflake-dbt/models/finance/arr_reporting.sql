{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--tested on snowflake for consistency of arr by month week and day
--tested for parent child relationships with amounts matching salesforce

with a as (
    select
    report_month as report_mo
    ,account_name
    ,account_id
    ,max(fiscal_year) as fiscal_yr
    ,max(fiscal_quarter) as fiscal_qtr
    ,max(report_week) as report_wk
    ,max(report_date) as report_day
    ,max(closing_date) as close_day
    ,min(parent_id) as parent_id
    ,min(split_part(parent_name,'-',1)) as parent
    ,min(account_owner) as account_owner
    ,max(description) as opportunity_description
    ,max(term_months) as term
    ,max(license_start_date) as license_beg
    ,max(license_end_date) as license_end
    ,sum(billing_amt) as tcv
    ,sum(first_yr_bill) as yr1_billing
    ,sum(opportunity_arr) as arr
    ,sum(expire_arr) as expire
	,sum(renew_arr) as arr_renewed
	,sum(arr_change) as arr_delta
    ,sum(arr_delta) over (partition by account_id order by report_mo rows between unbounded preceding and 1 preceding) as acct_beg_arr
    ,sum(arr_delta) over (partition by account_id order by report_mo) as acct_end_arr
    ,sum(new_arr) as new
    ,iff(arr_delta - new > 0 and acct_beg_arr =0,arr_delta - new,0) as resurrected
    ,iff(arr_delta - new <0 and acct_end_arr >0,arr_delta-new,0) as contracted
    ,iff(arr_delta - new <0 and acct_end_arr = 0,arr_delta-new,0) as churned
    ,iff(datediff('day',report_mo,current_date)>30,churned,0) as above30days_expired
    ,iff(arr_delta - new >0,arr_delta-new,0) as expanded
    ,sum(arr_delta) over (order by report_mo||report_day||account_id rows between unbounded preceding and 1 preceding) as total_beg_arr
    ,sum(arr_delta) over (order by report_mo||report_day||account_id) as total_end_arr
    ,total_end_arr - total_beg_arr as total_change
    ,iff(new>0,1,0) as cnt_new
    ,iff(resurrected>0,1,0) as cnt_resurrected
    ,iff(contracted<0,1,0) as cnt_contracted
    ,iff(expanded>0,1,0) as cnt_expanded
    ,iff(expire<0,-1,0) as cnt_expired
    ,iff(arr_renewed>0,1,0) as cnt_renewed
    ,iff(churned<0,-1,0) as cnt_churned
    ,iff(above30days_expired<0,-1,0) as cnt_above30_expired
    ,cnt_new + cnt_resurrected + cnt_expired + cnt_renewed as cnt_changed   
	,min(product) as product
	,min(plan) as plan
	,min(government) as government
	,min(customer_tier) as tier
	,min(company_type) as company_type
	,min(cosize) as company_size
	,min(industry) as industry
	,min(geo) as geo
	,min(country) as country
	,min(health_score) as health_score
	,last_day(min(account_start)) as cohort_month
    ,last_day(dateadd('month',1,last_day(dateadd('month',2,date_trunc('quarter',dateadd('month',-1,cohort_month)))))) as cohort_fiscal_qtr
    ,last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,cohort_month)))))) as cohort_fiscal_yr
from {{ ref( 'arr_transactions') }}
--from arr_transactions
	where report_mo <= last_day(current_date,month)
	group by 1,2,3
order by report_mo,close_day,account_id
)


--append with calculations and dimensions separately to avoid window nesting
select
	a.account_id||'-'||report_mo as unique_key
    ,a.*
    ,datediff('year',a.cohort_fiscal_yr,a.fiscal_yr) as fiscal_year_no
    ,round((datediff('day',a.cohort_month,fiscal_qtr))/30,0) as fiscal_month_no
    ,round((datediff('day',a.cohort_fiscal_qtr,fiscal_qtr))/90,0) as fiscal_quarter_no
    ,dense_rank() over (partition by a.account_id order by report_mo) as trans_no 
    ,sum(cnt_changed) over (order by report_mo||report_day||account_id) as active_customers
    ,round(avg(acct_end_arr) over (partition by a.parent_id,2)) as average_arr
    ,case 
        when average_arr <=10000 then '4_AvgARR_upto10K'
        when average_arr >10000 and average_arr <=100000 then '3_AvgARR_10Kupto100K'
        when average_arr >100000 and average_arr <=500000 then '2_AvgARR_100Kupto500K'
        when average_arr >500000 then '1_AvgARR_above500K'
        else null
    end as bin_avg_arr
from a

order by report_mo desc, report_day desc, account_id desc