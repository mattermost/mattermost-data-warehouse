{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--tested on snowflake for consistency of arr by month week and day
--tested for parent child relationships with amounts matching salesforce
--late renewal starts when renewal closes in the next month after expiry
--added logic for late renewal when close date is <=90 later that license start 
--and resurrection for cases when close date > license start
--Expansion and contraction calculated for late renewals but not for resurrections
--Revised late renewal and resurrection logic to remove waiting period 

with a as (
    select
    report_month as report_mo
    ,account_name
    ,account_id
    ,min(child_no) as child_no
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
    --renew_arr is based on the opportunity arr that is related to opportunity names with the word renewal 
	--renew_arr includes expansion amount if any
	,sum(renew_arr) as arr_renewed
	,sum(arr_change) as arr_delta
    ,coalesce(sum(arr_delta) over (partition by account_id order by license_beg, close_day rows between unbounded preceding and 1 preceding),0) as acct_beg_arr
    ,coalesce(sum(arr_delta) over (partition by account_id order by license_beg, close_day),0) as acct_end_arr
    ,sum(new_arr) as new
    ,case 

        --logic to specifically characterize DR technologies as an expansion and not a late renewal based on business combination with Wintermute
        when 
            arr_delta - new > 0 and acct_beg_arr =0 and account_id = '0013p00002AdrNFAAZ' 
            then 0
        --logic to specifically treat Bank of America transaction as late renewal as general logic does not accomodate multiple contracts due to lack of recording consistency
        when 
            account_id = '00136000015uBxoAAE' and report_month = date '2020-04-30' then arr_delta 
        --late renewal identifier
        --when arr delta is greater than new and acct beg balance is zero reflecting previous period churn with no renewal and no expiry in the same month
        when 
            arr_delta - new > 0 and acct_beg_arr = 0 and expire = 0 and datediff('day',license_beg,close_day) <=90
            then arr_delta - new 
        --retire previous logic
        /*when
            iff(arr_delta - new > 0 and acct_beg_arr =0 and account_id != '0013p00002AdrNFAAZ' ,arr_delta - new,0) > 0 
            and 
        --but elapsed time is under 90 days
            iff(arr_renewed>0,datediff('day',license_beg,close_day),0) <=90 
        --value is arr opportunity above new and only if previous arr was zero
        then iff(arr_delta - new > 0 and acct_beg_arr =0,arr_delta - new,0)*/
        else 0
     end as gross_late_renewal
    ,iff(gross_late_renewal>0,coalesce(last_value(arr_delta) over (partition by account_id order by report_mo rows between 2 preceding and 1 preceding),0),0) as previous_expire
    ,previous_expire*-1 as late_renewal
    ,case 
        --resurrection is a possibility
        when 
            arr_delta - new > 0 and acct_beg_arr =0 and expire = 0  and datediff('day',license_beg,close_day) >90
            then arr_delta - new 
        --retire previous logic to make symmetric with with late renewals
        /*when
            iff(arr_delta - new > 0 and acct_beg_arr =0,arr_delta - new,0) > 0 
            and 
        --but elapsed time is over 90 days
            iff(arr_renewed>0,datediff('day',license_beg,close_day),0) >90 
        --value is arr opportunity above new and only if previous arr was zero
        then iff(arr_delta - new > 0 and acct_beg_arr =0,arr_delta - new,0)*/
        else 0
     end as resurrected
    ,case 
        when gross_late_renewal > 0 and gross_late_renewal + previous_expire < 0
        then gross_late_renewal + previous_expire 
        else iff(arr_delta - new <0 and acct_end_arr != 0,arr_delta-new-resurrected,0) 
        end as contracted
    ,iff(arr_delta - new <0 and acct_end_arr = 0,arr_delta-new-resurrected,0) as churned
    --on time renewals based on original renewal amount
    ,iff(arr_renewed>0 and expire <0,(expire - churned -  contracted)*-1,0) as renewed
    ,case 
        when gross_late_renewal > 0 and gross_late_renewal + previous_expire > 0
        then gross_late_renewal + previous_expire 
        else iff(arr_delta - new - late_renewal >0,arr_delta-new-resurrected-late_renewal,0) 
     end as expanded
    ,sum(arr_delta) over (order by report_mo||report_day||account_id rows between unbounded preceding and 1 preceding) as total_beg_arr
    ,sum(arr_delta) over (order by report_mo||report_day||account_id) as total_end_arr
    ,total_end_arr - total_beg_arr as total_change
    ,iff(new>0,1,0) as cnt_new
    ,iff(late_renewal>0,1,0) as cnt_late_renewal
    ,iff(resurrected>0,1,0) as cnt_resurrected
    ,iff(contracted<0,1,0) as cnt_contracted
    ,iff(expanded>0,1,0) as cnt_expanded
    ,iff(expire<0,-1,0) as cnt_expired
    ,iff(arr_renewed>0,1,0) as cnt_renewed
    ,iff(churned<0,-1,0) as cnt_churned
    ,cnt_new + cnt_late_renewal + cnt_resurrected + cnt_churned as cnt_changed   
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
    ,dense_rank() over (partition by account_id order by license_beg, close_day) as trans_no
    ,datediff('day',license_beg,close_day) as closing_delay
    ,a.*
    ,datediff('year',a.cohort_fiscal_yr,a.fiscal_yr) as fiscal_year_no
    ,round((datediff('day',a.cohort_month,fiscal_qtr))/30,0) as fiscal_month_no
    ,round((datediff('day',a.cohort_fiscal_qtr,fiscal_qtr))/90,0) as fiscal_quarter_no
    ,sum(cnt_changed) over (order by report_mo||report_day||account_id) as active_customers
    ,round(avg(acct_end_arr) over (partition by a.account_id),2) as average_arr
    ,iff(acct_beg_arr>acct_end_arr,acct_beg_arr,acct_end_arr) as ref_arr
    ,last_value(ref_arr) over (partition by a.account_id order by license_beg,close_day ) as latest_arr
    ,case 
        when latest_arr >500000 then '1_AvgARR_above500K'
        when latest_arr <=500000 and latest_arr >100000 then '2_AvgARR_100Kupto500K'
        when latest_arr >10000 and latest_arr <=100000 then '3_AvgARR_10Kupto100K'
        else '4_AvgARR_upto10K'
     end as bin_avg_arr
    --positive when tcv is for more than a year
    --negative when tcv is for less than a year 
    ,(tcv-arr) as multi_yr
    ,(tcv) as gross_booking
    --netbooking also equal to new + resurrected + expanded + multi_yr
    ,(tcv-renewed-late_renewal) as net_booking
from a
order by report_mo desc, report_day desc, account_id desc