{{config({
    "materialized": "table",
    "schema": "finance",
    "tags": 'nightly'
  })
}}


with a as (
select
account_name
,account_owner
,account_id
,report_month
,sum(new_arr) as new
,sum(expire_arr) as expire
,sum(renew_arr) as renew
,sum(reduction_arr) as reduce
,sum(contract_expansion) as contract_expand
,sum(account_expansion) as account_expand
,sum(arr_change) as arr_delta
,new+expire+renew+reduce+contract_expand+account_expand - arr_delta as check_amt
from {{ ref( 'arr_transactions') }}
where report_month <= last_day(current_date)
group by 1,2,3,4
order by report_month
)

--query needed to calculate separately resurrection arr and churn_arr
select
a.account_name
,a.account_owner
,a.account_id
,a.report_month
,dense_rank() over (partition by account_id order by report_month) as trans_no 
,coalesce(sum(arr_delta) over (partition by account_id order by report_month rows between unbounded preceding and 1 preceding),0) as beg_arr
,sum(arr_delta) over (partition by account_id order by report_month) as end_arr
,case when trans_no = 1 then new else 0 end as new_arr
,case when trans_no !=1 then new else 0 end as resurrect_arr
--,expire as expire_arr
--,renew as renewal_arr
--,reduce as reduction_arr
,case when expire+renew+reduce >0 then expire+renew+reduce else 0 end as renewal_expand
,case when expire+renew+reduce <0 and end_arr = 0 then expire+renew + reduce else 0 end as churn
,case when expire+renew+reduce <0 and end_arr !=0 then expire+renew + reduce else 0 end as contraction
,contract_expand 
,account_expand 
,arr_delta
,check_amt
,new_arr + resurrect_arr + renewal_expand + churn +contraction + contract_expand + account_expand - arr_delta as check_2_amt
,case when end_arr >0 then 1 else 0 end as active_customer
,current_date as refresh_date
from a
order by account_id, report_month asc


