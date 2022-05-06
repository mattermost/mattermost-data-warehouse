{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--v4 of arr transactions 
--this query reflects arr transactions during the lifecycle of a paying self serve customer 
--based on key input fields by sales ops on license_start_date__c license_end_date__c and amount 
--in the salesforce opportunity table
--logic then calculates corresponding opportunity transactions to create an ending arr balance by report month and rollfoward of activities
--ending arr balance is validated by the aggreement of active licenses and ending arr per account id as of run date
--this query also corrects for the new customer count which raw data incorrectly reports because of migration cutoff
--opportunities closed are recognized at the later of close date or license start date
--license expiry is recognized at license end date even though early cancellation notice is received
--to identify cloud and monthly billing customers to be excluded from selfserve ARR
--currently account owner in accounts table is not reflective of the true account owner thus using the latest account owner of the opportunity
--structure of queries below are funnel data to selfserve arr then gather demographic info then pull master data set and add expiry and renewal information 


with cloud as (
   select 
        accountid,
        sum(case when lower(name) like '%cloud billing%' or lower(name) like '%monthly billing%' then 1 else 0 end) as cloud
    from  {{ ref( 'opportunity') }} opp
    --from analytics.orgm.opportunity opp
    where 
        opp.iswon in (true,false)
        and opp.isclosed = true
        and opp.isdeleted = false
    group by 1
    having cloud > 0
    order by 1 asc
)

--cte to limit report to paying customers and exclude trial customers that did not convert to paying
,pop as (
    select
        opp.accountid
        ,count(distinct id) as opportunities
        --,sum(coalesce(ending_arr__c,0)) as arr
        ,sum(coalesce(opp.amount,0)) as arr
    from  {{ ref( 'opportunity') }} opp
    --from analytics.orgm.opportunity opp
    WHERE opp.iswon in (true,false)
        and opp.isclosed = true
        and opp.isdeleted = false
        and opp.accountid not in (select distinct accountid from cloud )
    group by 1
    having arr >=1
    order by 1
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
  
--subquery to gather account demographics
,ACCT AS (
    SELECT
      COALESCE(A.PARENTID,A.SFID) AS PARENT_ID
      ,A.SFID AS ACCOUNT_ID
      ,coalesce(parent_s_parent_acount__c,a.name) as parent_name
      ,A.GOVERNMENT__C AS GOVERNMENT
      ,A.CUSTOMER_SEGMENTATION_TIER__C AS CUSTOMER_TIER
      ,COALESCE(A.GEO__C,A.TERRITORY_GEO__C) AS GEO
      ,A.BILLINGCOUNTRY AS COUNTRY
      ,A.COMPANY_TYPE__C AS COMPANY_TYPE
      ,A.SEATS_ACTIVE_LATEST__C as latest_seats
      ,A.HEALTH_SCORE__C AS HEALTH_SCORE
      ,A.TYPE
      ,A.COSIZE__C AS COSIZE
      ,A.INDUSTRY
      ,A.ACCOUNTSOURCE 
    FROM     {{ ref( 'account') }} A
    --FROM ANALYTICS.ORGM.ACCOUNT A
    where  ISDELETED = FALSE
    ORDER BY PARENT_ID, account_id
)
--master data query
,d as (
    select
      acct.name as account_name
      ,opp.accountid as account_id
      ,opp.id as opportunity_id
      ,opp.ownerid 
      ,last_day(iff(opp.closedate>opp.license_start_date__c,opp.closedate,opp.license_start_date__c)::date) as report_month
      ,closedate::date as close_date
      ,opp.license_start_date__c::date as license_start_date
      --override for data input error
      ,case 
          when opp.id = '0063p00000zBnnuAAC' then date '2022-03-31' else opp.license_end_date__c::date end as license_end_date
      ,opp.license_active__c as license_active_sf
      --snowflake does not calculate date diff for months with decimals thus calculating on days 
      --common to see license periods adjusted for co terminating deals
      ,round(datediff('day',license_start_date,license_end_date)/(365/12),0) as term
      ,opp.iswon
      ,opp.type as opp_type
      ,case when opp.new_logo__c is null then 'No' else opp.new_logo__c end as new_logo
      ,round(coalesce(opp.amount,2),0) as bill_amt
      ,case when term = 12 then bill_amt else round(div0(bill_amt,term)*12,2) end as opportunity_arr
      ,opp.name as description
      --this field is not consistently populated and could be overwritten
      ,opp.original_opportunityid__c as original_id_renewed
    --from analytics.orgm.opportunity opp
	  --left join ANALYTICS.ORGM.ACCOUNT acct
    from {{ ref( 'opportunity') }} opp
    left join   {{ ref( 'account') }} acct 
        on acct.sfid = opp.accountid
    WHERE opp.iswon =true
        and opp.isclosed = true
        and opp.isdeleted = false
        and opp.accountid in (select distinct accountid from pop)
    order by opp.accountid, opp.closedate asc
)

--determining expiring renewals against the won renewals
--matching keys of expiring and renewals using for expiring the license end date and for renewals the start date won renewals
--expiration net of renewals
,r as (
--subquery for expiring licenses up to current month_end
select
    d.account_name
    ,d.account_id
    ,null as opportunity_id
    ,null as close_date
    ,dateadd('day',1,license_end_date) as lic_start_date
    ,dateadd('year',1,license_end_date) as lic_end_date
    ,12 as term
    ,false as iswon
    ,0 as bill_amt
    ,0 as opportunity_arr
    ,d.opportunity_arr *-1 as expired_arr
    ,0 as renewed_arr
    ,date_part('year',d.license_end_date)||'-'
      ||rank() over (partition by account_id, date_trunc('year',d.license_end_date)
      order by date_trunc('month',d.license_end_date)) 
      ||'-'||account_id 
      as match_key
    ,opportunity_id as original_id_renewed
from d
where license_end_date <= last_day(current_date)

union

--renewals won subquery
select
  d.account_name
  ,d.account_id
  ,d.opportunity_id as opportunity_id
  ,close_date
  ,license_start_date as lic_start_date
  ,license_end_date as lic_end_date
  ,term
  ,iswon
  ,bill_amt
  ,opportunity_arr
  ,0 as expired_arr
  ,d.opportunity_arr as renewed_arr
  ,date_part('year',d.license_start_date)||'-'
    ||rank() over (partition by account_id, date_trunc('year',d.license_start_date) 
    order by date_trunc('month',d.license_start_date)) 
    ||'-'||account_id
    as match_key
   ,original_id_renewed
from d
    where opp_type = 'Renewal' 
order by account_id, match_key asc
)  
 
--unites renewal and expiring data with the main opportunity transactions
,master as (
    select
      account_name
      ,match_key
      ,account_id
      ,opportunity_id
      ,max(case when iswon = true then close_date else null end) as close_date
      ,max(lic_start_date) as license_start
      ,max(lic_end_date) as license_end
      ,max(iswon) as is_won
      ,max(case when iswon = true then 'Renewal' else 'Expired' end) as opp_type
      ,max(case when iswon = true then term else 0 end) as term
      ,sum(bill_amt) as billing_amt
      ,sum(opportunity_arr) as opportunity_arr
      ,sum(0) as new_arr
      ,sum(expired_arr) as expire_arr
      ,sum(renewed_arr) as renew_arr
      ,sum(0) as contract_expand
      ,sum(0) as account_expand
    from r
    group by 1,2,3,4

    union

    select
    account_name
      ,null as match_key
      ,account_id
      ,opportunity_id
      ,close_date
      ,license_start_date as license_start 
      ,license_end_date as license_end
      ,iswon as is_won
      ,opp_type 
      ,term
      ,bill_amt as billing_amt
      ,opportunity_arr 
      ,case when lower(opp_type) like 'new%' then opportunity_arr else 0 end as new_arr
      ,0 as expire_arr
      ,0 as renew_arr
      ,case when opp_type = 'Contract Expansion' then opportunity_arr else 0 end as contract_expand
      ,case when opp_type = 'Account Expansion' then opportunity_arr else 0 end as account_expand
      from d
    where opp_type != 'Renewal'
    order by account_name, license_start, close_date
) 
,
--subquery to determine max license excluding expired 
licterm as (
select
    account_id
    ,max(license_end) as license_term
from master
where opp_type != 'Expired'
group by 1
order by 1
)  

--aggregating table to output
select
    master.account_name
    ,master.account_id
    ,master.opportunity_id
    --use actual dates for won renewals and calc dates for expired renewals
    ,coalesce(l.start_date, master.license_start) as license_start_date
    ,coalesce(l.end_date, master.license_end) as license_end_date
    ,min(license_start_date) over (partition by master.account_id) as account_start
    ,licterm.license_term as max_license
    --tenure yr for ltv when based on the license anniversary beg date
    ,datediff('year',account_start,license_start_date) as beg_tenure_yr
    --tenure yr for ltv when based on the license anniversary end date
    ,datediff('year',account_start,license_end_date) as end_tenure_yr
    --when renewal expired close date is null
    ,coalesce(l.close_date,license_start_date) as closing_date
    ,dense_rank() over (partition by master.account_id order by closing_date,master.opportunity_id) as trans_no
    ,last_day(iff(closing_date>license_start_date,closing_date,license_start_date)) as report_month
    ,iff(closing_date>license_start_date,closing_date,license_start_date) as report_date
    ,last_day(dateadd('month',1,last_day(dateadd('month',2,date_trunc('quarter',dateadd('month',-1,report_month)))))) as fiscal_quarter
    ,last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,report_month)))))) as fiscal_year
    ,case
      when opp_type != 'Expired' and license_end_date >= last_day(current_date) and license_start_date <= last_day(current_date) then true 
      else false 
      end as license_active_calc
    ,coalesce(l.license_active_sf,false) as license_activesf
    ,iff(license_active_calc=license_activesf,true,false) as status_aligned
    ,is_won
    ,opp_type
    ,coalesce(l.new_logo,'No') as newlogo
    ,term as term_months
    ,billing_amt
    ,opportunity_arr
    ,new_arr
    ,expire_arr
    ,renew_arr
    ,case 
        when opp_type = 'Reduction' then opportunity_arr
        else 0 end as reduction_arr
    ,contract_expand as contract_expansion
    ,account_expand as account_expansion
    ,case when report_month > last_day(current_date) then 0 else
        new_arr + expire_arr + renew_arr + reduction_arr + contract_expansion + account_expansion end as arr_change
    ,sum(arr_change) over (partition by master.account_id order by closing_date) as ending_arr
    ,case when license_active_calc = true and is_won = true
        then opportunity_arr 
        else 0 
     end as active_arr
    ,l.description
    ,acct.parent_id
    ,acct.parent_name
    ,government
    ,customer_tier
    ,company_type
    ,cosize
    ,industry
    ,accountsource
    ,geo
    ,country
    ,type
    ,health_score 
    --below is the sales rep of the latest closed deal
    ,b.name as account_owner
    ,u.name as opportunity_owner
    --,case when ending_arr = 0 then 0 else latest_seats end as current_seats
    ,current_date as date_refreshed
    from master
    left join 
      (select distinct opportunity_id, ownerid, close_date, license_start_date as start_date, license_end_date as end_date, license_active_sf, description, new_logo from d) l 
      on l.opportunity_id = master.opportunity_id
    left join acct on acct.account_id = master.account_id
    left join b on b.accountid = master.account_id
    left join licterm on licterm.account_id = master.account_id
    left join analytics.orgm.user u on u.sfid=l.ownerid
    order by master.account_name,trans_no

