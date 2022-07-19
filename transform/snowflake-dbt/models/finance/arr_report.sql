{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}


WITH A AS (    
SELECT
    last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,report_month)))))) as fiscal_year
    ,report_month
    ,sum(tcv) as tcv_signed
    ,sum(arr) as arr_signed
    ,sum(arr_delta) as arr_change
    ,sum(new_arr) as new
    ,sum(resurrect_arr) as resurrected
    ,sum(expire) as arr_expired
    ,sum(churn) - arr_expired as renewed
    ,sum(churn) as churned
    ,sum(total_expansion) as expanded
    ,sum(contraction) as contracted

    ,sum(cnt_new_account) as new_cnt
    ,sum(cnt_resurrect) as resurrect_cnt
    ,sum(iff(expire!=0,1,0))*-1 as expire_cnt
    ,sum(cnt_churn) - expire_cnt as renew_cnt
    ,sum(cnt_churn) as churn_cnt
    ,sum(cnt_change) as change_cnt
    ,sum(cnt_total_expand) as expand_cnt
    ,sum(cnt_contraction) as contract_cnt

--FROM analytics.finance.arr_rollforward
from {{ ref( 'arr_rollforward') }}
GROUP by 1,2


UNION

SELECT
    FISCAL_YEAR
    ,REPORT_MONTH
    ,SUM(BILLING_AMT) AS TCV_SIGNED
    ,SUM(OPPORTUNITY_ARR) AS ARR_SIGNED
    ,SUM(ARR_CHANGE) AS ARR_CHANGE
    ,SUM(NEW_ARR) AS NEW
    ,SUM(0) as RESURRECTED
    ,SUM(EXPIRE_ARR) AS ARR_EXPIRED
    ,SUM(RENEW_ARR) AS ARR_RENEWED
    ,ARR_EXPIRED + ARR_RENEWED AS CHURNED
    ,SUM(CONTRACT_EXPANSION) + SUM(ACCOUNT_EXPANSION) AS EXPANDED
    ,SUM(REDUCTION_ARR) AS CONTRACTED
    ,SUM(iff(new_arr>0,1,0)) AS NEW_CNT
    ,SUM(0) AS RESURRECT_CNT
    ,SUM(IFF(EXPIRE_ARR=0,0,1))*-1 AS EXPIRE_CNT
    ,SUM(IFF(RENEW_ARR=0,0,1)) AS RENEW_CNT
    ,EXPIRE_CNT + RENEW_CNT AS CHURN_CNT
    ,NEW_CNT + RESURRECT_CNT + EXPIRE_CNT + RENEW_CNT AS CHANGE_CNT
    ,SUM(IFF(CONTRACT_EXPANSION + ACCOUNT_EXPANSION > 0,1,0)) AS expand_cnt
    ,SUM(IFF(REDUCTION_ARR<0,1,0)) AS contract_cnt
--FROM ANALYTICS.FINANCE.ARR_TRANSACTIONS
from {{ ref( 'arr_transactions') }}
WHERE REPORT_MONTH = LAST_DAY(CURRENT_DATE,'MONTH')
GROUP BY 1,2
ORDER BY 1,2
)

,inv as (
select
report_month,
round(sum(iff(term_months < 12, billing_amt, billing_amt/term_months*12)),2) as year1_inv_amt
--from finance.arr_transactions
from {{ ref( 'arr_transactions') }}
where is_won = true
and report_month <= last_day(current_date,month)
group by 1
order by report_month desc
)

SELECT
    a.* 
    ,inv.year1_inv_amt
    ,round(sum(arr_change) over (order by a.report_month asc),2) as end_arr
    ,sum(change_cnt) over (order by a.report_month asc) as end_customers
    ,sum(change_cnt) over (order by a.report_month asc rows between unbounded preceding and 1 preceding) as beg_customers
    ,div0(new,new_cnt)::int as avg_new
    ,div0(expanded,expand_cnt)::int as avg_expand
    ,div0(contracted,contract_cnt)::int as avg_contract
    ,div0(churned,churn_cnt)::int as avg_churn
    ,round(div0(renew_cnt,expire_cnt*-1),2) as renew_rate
    ,round(div0(churn_cnt,beg_customers),2) as churn_rate
FROM A
LEFT JOIN INV ON INV.REPORT_MONTH = A.REPORT_MONTH