{{config({
    "materialized": "table",
    "schema": "finance",
    "tags": 'nightly'
  })
}}


--cte to gather master arr transaction data and collapse by report month and thru lastest month end
--this data will then be grouped to identify resurrection churn expansion and contraction at a higher level
with a as (
    select
      account_name
      ,account_id
      ,report_month
      ,fiscal_quarter
      ,fiscal_year
      ,account_owner
      ,max(opportunity_owner) as opp_owner
      ,max(newlogo) as new_logo
      ,min(account_start) as cohort
      ,max(customer_tier) as tier
      ,max(company_type) as co_type
      ,max(industry) as industry
      ,max(accountsource) as account_source
      ,max(country) as nation
      ,max(health_score) as healthscore
      ,max(geo) as geography
      ,max(government) as gov
      ,min(license_start_date) as license_beg
      ,max(license_end_date) as license_end
      ,sum(billing_amt) as tcv
      ,sum(opportunity_arr) as arr
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
    group by 1,2,3,4,5,6
    order by report_month, account_id
)

--query needed to calculate separately resurrection arr and churn_arr
select
    a.account_name
    ,a.account_id
    ,a.account_owner
    ,a.report_month
    ,a.fiscal_quarter
    ,a.fiscal_year
    ,a.opp_owner as opportunity_owner
    ,datediff('year',cohort,a.fiscal_year) as fiscal_year_no
    ,dense_rank() over (partition by account_id order by report_month) as trans_no 
    ,a.license_beg
    ,a.license_end
    ,tcv
    ,arr
    ,expire
    ,arr_delta
    ,coalesce(sum(arr_delta) over (partition by account_id order by report_month rows between unbounded preceding and 1 preceding),0) as beg_arr
    ,sum(arr_delta) over (partition by account_id order by report_month) as end_arr
    --finding that salesforce does not consistently classify arr deals into the proper category
    --discovered that because of sfdc migration not all first transactions are new subscription
  
    ,case when trans_no = 1 then arr_delta else 0 end as new_arr
    ,case when trans_no !=1 and beg_arr = 0 and arr_delta >0 then arr_delta else 0 end as resurrect_arr
  
    ,case when trans_no !=1 and resurrect_arr = 0 and expire+renew+reduce >0 then expire+renew+reduce else 0 end as renewal_expand
    ,case 
        when trans_no !=1  and contract_expand >0 and resurrect_arr = 0 then contract_expand + new
        when trans_no !=1  and contract_expand =0 and resurrect_arr = 0 and a.new_logo != 'Yes' then new 
        else 0 
        end as contract_expansion
    ,renewal_expand + contract_expansion as annual_expansion
    
    ,case 
        when trans_no !=1 and account_expand > 0 and resurrect_arr = 0 then account_expand + new
        when trans_no !=1 and account_expand = 0 and a.new_logo = 'Yes' and resurrect_arr=0 then new 
        else 0 
        end as account_expansion
    --discussing with finance total expansion makes more sense than segments of expansion split up before
    ,account_expansion + annual_expansion as total_expansion
  
    ,case when trans_no !=1  and expire+renew+reduce <0 and end_arr !=0 then expire+renew + reduce else 0 end as contraction
    
    ,case when expire+renew+reduce <0 and end_arr = 0 then expire+renew + reduce else 0 end as churn

    --checks that all activity before grouping balances with the change in arr
    ,check_amt as val_check1
    --that all grouping adds up to change in arr
    ,new_arr + resurrect_arr + renewal_expand + churn +contraction + contract_expansion + account_expansion - arr_delta as val_check2
  
    ,case when trans_no = 1 then 1 else 0 end as cnt_new_account
    ,case when resurrect_arr > 0 then 1 else 0 end as cnt_resurrect
    ,case when churn <0 then -1 else 0 end as cnt_churn
    ,cnt_new_account + cnt_resurrect + cnt_churn as cnt_change
    ,case 
      when trans_no = 1 then 1 
      when trans_no !=1 and end_arr > 0 and license_end >= report_month then 1 
      else 0 
      end as cnt_active_customer
    ,case when renewal_expand + contract_expansion > 0 then 1 else 0 end as cnt_annual_expand
    ,case when account_expansion > 0 then 1 else 0 end as cnt_account_expand
    ,cnt_annual_expand + cnt_account_expand as cnt_total_expand
    ,case when contraction <0 then 1 else 0 end as cnt_contraction
    ,current_date as refresh_date
    ,a.account_id||' '||report_month as unique_key
    ,a.cohort
    ,a.tier
    ,a.co_type
    ,a.industry
    ,a.geography
    ,a.nation
    ,a.gov
    ,a.new_logo
from a
order by cohort, account_id, report_month asc



