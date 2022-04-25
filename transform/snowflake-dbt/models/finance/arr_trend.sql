{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}


--this query captures summary monthly activities that can be grouped to fiscal year and fiscal quarter based on the arr_transactions table


with t as (
select
		distinct report_month, fiscal_year, fiscal_quarter
from analytics.finance_dev.arr_transactions
where report_month <= last_day(current_date)
order by 1 asc
)
--subquery for data pull and aggregation of activities
,a as (
select
		account_name
		,account_owner
		,account_id
        ,parent_name
        ,parent_id
		,customer_tier
		,industry
		,geo
		,government
		,report_month
		,fiscal_quarter
		,fiscal_year
		,coalesce(new_logo,'No') as newlogo_flagged
		,min(fiscal_year) as fiscal_yr_join
		,sum(new_arr) as new
		,sum(expire_arr) as expire
		,sum(renew_arr) as renew
		,sum(reduction_arr) as reduce
		,sum(contract_expansion) as contract_expand
		,sum(account_expansion) as account_expand
		,sum(arr_change) as arr_delta
		,new+expire+renew+reduce+contract_expand+account_expand - arr_delta as check_amt
		from analytics.finance_dev.arr_transactions
group by 1,2,3,4,5,6,7,8,9,10,11,12,13
order by report_month
)
--subquery to index transactions and use window functions to determine churn and resurrection events
,b as (
select
		a.account_name
		,a.account_owner
		,a.account_id
        ,parent_name
        ,parent_id
		,a.customer_tier
		,a.industry
		,a.geo
		,a.government
		,a.fiscal_yr_join
		,a.fiscal_year
		,a.fiscal_quarter
		,a.report_month
		,a.newlogo_flagged
		,dense_rank() over (partition by account_id order by report_month) as trans_no 
		,coalesce(sum(arr_delta) over (partition by account_id order by report_month rows between unbounded preceding and 1 preceding),0) as beg_arr
		,sum(arr_delta) over (partition by account_id order by report_month) as end_arr
		,case when trans_no = 1 then new else 0 end as new_arr
        ,case when trans_no !=1 then new else 0 end as resurrect_arr
		,case when expire+renew+reduce >0 then expire+renew+reduce else 0 end as renewal_expand
		,case when expire+renew+reduce <0 and end_arr = 0 then expire+renew + reduce else 0 end as churn
		,case when expire+renew+reduce <0 and end_arr !=0 then expire+renew + reduce else 0 end as contraction
        ,coalesce(contract_expand,0) as contract__expand
		,account_expand 
		,arr_delta
		,check_amt
		,new_arr + resurrect_arr + renewal_expand + churn +contraction + contract__expand + account_expand - arr_delta as check_2_amt
		,case when end_arr >0 then 1 else 0 end as active_customer
		,current_date as refresh_date
from a
order by account_id, report_month asc
)
--join report month and fiscal periods with cte b so that all months are accounted for in case no activity happens in one month
,report as (
select
		t.report_month 
        ,t.fiscal_year
        ,t.fiscal_quarter
		,b.account_name
		,b.account_owner
		,b.account_id
        ,b.parent_name
        ,b.parent_id
		,b.customer_tier
		,b.industry
		,b.geo
		,b.government
		,b.fiscal_yr_join
		,case when coalesce(b.new_arr,0)>0 then 'Yes' else coalesce(b.newlogo_flagged,'No') end as new_logo
        ,trans_no
		,beg_arr
        ,coalesce(b.arr_delta,0) as arr_change
        ,end_arr
		,coalesce(b.new_arr,0) as new
		,coalesce(b.resurrect_arr,0) as resurrected
		,coalesce(b.renewal_expand,0) as expand_renewal
		,coalesce(b.contract__expand,0) as expand_contract
		,coalesce(b.account_expand,0) as expand_account
		,expand_renewal + expand_contract + expand_account as expand_total
		,coalesce(b.churn,0) as churned
		,coalesce(b.contraction,0) as contraction_renewal
		,coalesce(check_amt,0) + coalesce(check_2_amt,0) as check_error
        ,iff(new>0,1,0) as cnt_new_sub
        ,iff(newlogo_flagged='Yes',1,0) as cnt_new_logo
        ,iff(resurrected>0,1,0) as cnt_resurrect
        ,iff(expand_total>0,1,0) as cnt_expansion
        ,iff(contraction_renewal<0,1,0) as cnt_contraction
        ,iff(churned<0,1,0) as cnt_churned
		,refresh_date
from t
left join b on b.report_month = t.report_month
)

--quarter and year flags are for looker filters to report on ending balance activity
--does a rollfoward of arr and active customer count by report month at an aggregate level
--previous rollfoward of cte b above was at an account id level
select
        fiscal_year
        ,fiscal_quarter
        ,report_month
        ,case when report_month = fiscal_quarter then 1 else 0 end as quarter_flag
        ,case when report_month = fiscal_year then 1 else 0 end as year_flag
        ,sum(arr_change) as arr_growth
        ,sum(arr_growth) over (order by report_month) as ending_arr
        ,sum(cnt_new_sub) as cnt_new_account
        ,sum(cnt_new_logo) as cnt_new_logo
        ,sum(cnt_resurrect) as cnt_resurrect
        ,sum(cnt_expansion) as cnt_expansion
        ,sum(cnt_contraction) as cnt_contraction
        ,sum(cnt_churned) as cnt_churned
        ,sum(cnt_new_sub) + sum(cnt_resurrect) -sum(cnt_churned) as cnt_change
        ,sum(cnt_change) over(order by report_month) as cnt_customers
        ,sum(new) as new_amt
        ,sum(resurrected) as resurrected_amt
        ,sum(expand_total) as expansion_amt
        ,sum(churned) as churned_amt
        ,sum(contraction_renewal) as contraction_amt
        ,round(ending_arr/cnt_customers,0) as avg_arr
        ,round(div0(new_amt,cnt_new_account),0) as avg_new_account
        ,round(div0(sum(resurrected),sum(cnt_resurrect)),0) as avg_resurrected
        ,round(div0(sum(expand_total),sum(cnt_expansion)),0) as avg_expansion
        ,round(div0(sum(churned),sum(cnt_churned)),0) as avg_churned
        ,round(div0(sum(contraction_renewal),sum(cnt_contraction)),0) as avg_contraction
from report
where report_month <= last_day(current_date)
group by 1,2,3,4,5
order by 3 desc










