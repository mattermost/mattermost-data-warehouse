{{config({
    "materialized": "table",
    "schema": "finance",
    "tags": 'nightly'
  })
}}

--this query transforms arr rollforward to be useful for ltv calculations

with b as (
select
    account_id,
    account_name,
    date_trunc('month',account_start) as cohort_month,
    last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,cohort_month)))))) as fiscal_year,
    datediff('year',account_start,license_start_date) as tenure_yr,
    geo,
    customer_tier,
    industry,
    account_owner,
    count(distinct account_id) as acct_cnt,
    sum(new_arr) as new,
    sum(arr_change) as arr_delta
  --from analytics.finance.arr_transactions
  from {{ ref( 'arr_transactions') }}
  where report_month <= last_day(dateadd('month',-1,current_date))
group by 1,2,3,4,5,6,7,8,9
order by cohort_month, account_name, tenure_yr asc
)

select
b.*,
sum(arr_delta) over (partition by account_id order by tenure_yr) as end_arr
from b