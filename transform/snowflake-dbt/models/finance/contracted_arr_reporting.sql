{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--v1 Roy CONTRACTED ARR REPORTING
--tested on snowflake 20230606
--this sql model reports ARR on date contracted date instead of report date used on arr_reporting model
--Contracted date is based on the max of closing day which is the date the sales person closed the deal with approved contracts
--Contracted date is the max of closing days within a certain period
--There is a potential for several closing days for the same period that pertained to the same deal and doing the maximum avoids potential duplicates


--population coming from arr_transactions
--grouping arr transactions to the same close day to report contracted arr and the associated breakdown of arr change
--cte calculatings daily outstanding arr for whole portfolio
with a as (
    select
    closing_month as closing_mo
    ,account_name
    ,account_id
    ,min(child_no) as child_no
    ,max(closing_fiscal_year) as closing_fiscal_yr
    ,max(closing_fiscal_quarter) as closing_fiscal_qtr
    ,max(fiscal_year) as report_fiscal_yr
    ,max(fiscal_quarter) as report_fiscal_qtr
    ,max(closing_month) as closing_month
    ,max(closing_week) as closing_wk
    ,max(closing_date) as close_day
    ,max(report_month) as report_month
    ,min(parent_id) as parent_id
    ,min(split_part(parent_name,'-',1)) as parent
    ,min(account_owner) as account_owner
    ,max(description) as opportunity_description
    ,max(term_months) as term
    ,max(license_start_date) as license_beg
    ,max(license_end_date) as license_end
    ,sum(billing_amt) as tcv
    ,sum(first_yr_bill) as yr1_billing
    ,sum(opportunity_arr) as carr
    ,sum(expire_arr) as expire
	,sum(renew_arr) as arr_renewed
	,sum(arr_change) as arr_delta
    ,coalesce(sum(arr_delta) over (partition by account_id order by license_beg, close_day rows between unbounded preceding and 1 preceding),0) as acct_beg_carr
    ,coalesce(sum(arr_delta) over (partition by account_id order by license_beg, close_day),0) as acct_end_carr
    ,sum(new_arr) as new
    ,case 

        --logic to specifically characterize DR technologies as an expansion and not a late renewal based on business combination with Wintermute
        when 
            arr_delta - new > 0 and acct_beg_carr =0 and account_id = '0013p00002AdrNFAAZ' 
            then 0
        --logic to specifically treat Bank of America transaction as late renewal as general logic does not accomodate multiple contracts due to lack of recording consistency
        when 
            account_id = '00136000015uBxoAAE' and closing_mo = date '2020-04-30' then arr_delta 
        --late renewal identifier
        --when arr delta is greater than new and acct beg balance is zero reflecting previous period churn with no renewal and no expiry in the same month
        when 
            arr_delta - new > 0 and acct_beg_carr = 0 and expire = 0 and datediff('day',license_beg,close_day) <=90
            then arr_delta - new 
        else 0
     end as gross_late_renewal
    ,iff(gross_late_renewal>0,coalesce(last_value(arr_delta) over (partition by account_id order by closing_mo rows between 2 preceding and 1 preceding),0),0) as previous_expire
    ,previous_expire*-1 as late_renewal
    ,case 
        --resurrection is a possibility
        when 
            arr_delta - new > 0 and acct_beg_carr =0 and expire = 0  and datediff('day',license_beg,close_day) >90
            then arr_delta - new 
        else 0
     end as resurrected
    ,case 
        when gross_late_renewal > 0 and gross_late_renewal + previous_expire < 0
        then gross_late_renewal + previous_expire 
        else iff(arr_delta - new <0 and acct_end_carr != 0,arr_delta-new-resurrected,0) 
        end as contracted
    ,iff(arr_delta - new <0 and acct_end_carr = 0,arr_delta-new-resurrected,0) as churned
    --on time renewals based on original renewal amount
    ,iff(arr_renewed>0 and expire <0,(expire - churned -  contracted)*-1,0) as renewed
    ,case 
        when gross_late_renewal > 0 and gross_late_renewal + previous_expire > 0
        then gross_late_renewal + previous_expire 
        else iff(arr_delta - new - late_renewal >0,arr_delta-new-resurrected-late_renewal,0) 
     end as expanded
    ,sum(arr_delta) over (order by closing_mo||close_day||account_id rows between unbounded preceding and 1 preceding) as total_beg_carr
    ,sum(arr_delta) over (order by closing_mo||close_day||account_id) as total_end_carr
    ,total_end_carr - total_beg_carr as total_change
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
--from analytics.finance.arr_transactions
	where closing_mo <= last_day(current_date,month)
	group by 1,2,3
--order by closing_mo,close_day,account_id
)

--append with calculations and dimensions separately to avoid window nesting
--dimensions added for further analyses of composition cohorts and bins
select
	a.account_id||'-'||closing_mo as unique_key
    ,dense_rank() over (partition by account_id order by license_beg, close_day) as trans_no
    ,case when close_day > license_beg then datediff('day',license_beg,close_day) else 0 end as days_delayed
    ,case 
        when report_month < closing_month then 'Past Month'
        when report_month > closing_month then 'Future Month'
		else 'Current Month'
		end as license_month
    ,case 
        when closing_fiscal_qtr > report_fiscal_qtr then 'Past Quarter'
        when closing_fiscal_qtr < report_fiscal_qtr then 'Future Quarter'
		else 'Current Quarter'
		end as license_quarter
    ,a.*
    ,datediff('year',a.cohort_fiscal_yr,a.closing_fiscal_yr) as cfiscal_year_no
    ,round((datediff('day',a.cohort_month,closing_fiscal_qtr))/30,0) as cfiscal_month_no
    ,round((datediff('day',a.cohort_fiscal_qtr,closing_fiscal_qtr))/90,0) as cfiscal_quarter_no
    ,sum(cnt_changed) over (order by closing_mo||close_day||account_id) as cactive_customers
    --reference arr for bins
    ,round(avg(acct_end_carr) over (partition by a.account_id),2) as average_arr
    ,iff(acct_beg_carr>acct_end_carr,acct_beg_carr,acct_end_carr) as ref_arr
    ,last_value(ref_arr) over (partition by a.account_id order by license_beg,close_day ) as latest_arr
    ,case 
        when latest_arr >500000 then '1_AvgARR_above500K'
        when latest_arr <=500000 and latest_arr >100000 then '2_AvgARR_100Kupto500K'
        when latest_arr >10000 and latest_arr <=100000 then '3_AvgARR_10Kupto100K'
        else '4_AvgARR_upto10K'
     end as bin_avg_arr
    ,iff(tcv>carr,(tcv-carr),0) as cmulti_yr
    ,(tcv) as cgross_booking
    --netbooking also equal to new + resurrected + expanded + multi_yr
    ,(tcv-renewed-late_renewal) as cnet_booking
from a
order by closing_mo desc, close_day desc, account_id desc

