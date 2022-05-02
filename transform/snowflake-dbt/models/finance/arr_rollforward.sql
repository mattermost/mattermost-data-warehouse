{{config({
    "materialized": "table",
    "schema": "finance",
    "tags": 'nightly'
  })
}}


with a as (
    select
      account_name
      ,account_id
      ,report_month
      ,max(newlogo) as new_logo
      ,min(account_start) as cohort
      ,max(customer_tier) as tier
      ,max(company_type) as co_type
      ,max(industry) as industry
      ,max(geo) as geography
      ,max(government) as gov
      ,max(license_end_date) as latest_license_end
      ,sum(new_arr) as new
      ,sum(expire_arr) as expire
      ,sum(renew_arr) as renew
      ,sum(reduction_arr) as reduce
      ,sum(contract_expansion) as contract_expand
      ,sum(account_expansion) as account_expand
      ,sum(arr_change) as arr_delta
      ,new+expire+renew+reduce+contract_expand+account_expand - arr_delta as check_amt
    from {{ ref( 'arr_transactions') }}
    --from analytics.finance.arr_transactions
        where report_month <= last_day(current_date)
    group by 1,2,3
    order by report_month
)
--cte to determine who is the latest sales rep of an account based on last opportunity closed   
,o as (
    select 
      distinct o.accountid
      ,last_value(o.ownerid) over (partition by o.accountid order by o.closedate asc) as last_owner
    from {{ ref( 'opportunity') }} o
    --from analytics.orgm.opportunity o
      where o.isclosed = true
      and o.iswon = true
)
--cte that references name to ownerid found above on o
,b as (
    select
      o.accountid
      ,o.last_owner
      ,u.name
    from o
    left join analytics.orgm.user u on u.sfid = o.last_owner
)  
  

--query needed to calculate separately resurrection arr and churn_arr
select
a.account_name
,b.name as latest_account_owner
,a.account_id
,a.report_month
,a.latest_license_end
,last_day(dateadd('month',1,last_day(dateadd('month',2,date_trunc('quarter',dateadd('month',-1,report_month)))))) as fiscal_quarter
,last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,report_month)))))) as fiscal_year
,dense_rank() over (partition by account_id order by report_month) as trans_no 
,coalesce(sum(arr_delta) over (partition by account_id order by report_month rows between unbounded preceding and 1 preceding),0) as beg_arr
,sum(arr_delta) over (partition by account_id order by report_month) as end_arr
--finding that mattermost does not consistently classify arr deals into the proper category
--discovered that because of sfdc migration not all first transactions are new subscriptions
,case when trans_no = 1 then arr_delta else 0 end as new_arr
,case when trans_no !=1 and beg_arr = 0 and arr_delta >0 then arr_delta else 0 end as resurrect_arr
,case when trans_no !=1 and resurrect_arr = 0 and expire+renew+reduce >0 then expire+renew+reduce else 0 end as renewal_expand
,case when expire+renew+reduce <0 and end_arr = 0 then expire+renew + reduce else 0 end as churn
,case when trans_no !=1  and expire+renew+reduce <0 and end_arr !=0 then expire+renew + reduce else 0 end as contraction
--logic below normalizes practice for new logo vs new subscription
,case 
  when trans_no !=1  and contract_expand >0 and resurrect_arr = 0 then contract_expand + new
  when trans_no !=1  and contract_expand =0 and resurrect_arr = 0 and a.new_logo != 'Yes' then new 
  else 0 end as contract_expansion
--,account_expand 
,case 
  when trans_no !=1 and account_expand > 0 and resurrect_arr = 0 then account_expand + new
  when trans_no !=1 and account_expand = 0 and a.new_logo = 'Yes' and resurrect_arr=0 then new 
  else 0 end as account_expansion
,arr_delta
--that all activity before grouping balances with the change in arr
,check_amt
--that all grouping adds up to change in arr
,new_arr + resurrect_arr + renewal_expand + churn +contraction + contract_expansion + account_expansion - arr_delta as check_2_amt
,case when trans_no = 1 then 1 else 0 end as cnt_new_account
,case when resurrect_arr > 0 then 1 else 0 end as cnt_resurrect
,case when churn <0 then -1 else 0 end as cnt_churn
,cnt_new_account + cnt_resurrect + cnt_churn as cnt_change
,case 
  when trans_no = 1 then 1 
  when trans_no !=1 and end_arr > 0 and latest_license_end >= report_month then 1 
  else 0 
  end as cnt_active_customer
,case when renewal_expand + contract_expansion + account_expansion >0 then 1 else 0 end as cnt_expand
,case when contraction <0 then 1 else 0 end as cnt_contraction
,current_date as refresh_date
,a.account_id||' '||report_month as unique_key
,a.cohort
,a.tier
,a.co_type
,a.industry
,a.geography
,a.gov
,a.new_logo
from a
  left join b on b.accountid = a.account_id
order by account_id, report_month asc


