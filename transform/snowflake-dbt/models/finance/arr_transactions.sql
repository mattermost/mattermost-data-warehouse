{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}

--v5 of arr transactions 
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
--modified to have parent child relationship as deployed by Jim K

--identify mrr population as tracked by stripe and product type
with mrr as (
   select 
        es.server_id,
        ls.server_id as ls_server_id,
        ls.license_id,
        ls.customer_id,
        ls.id,
        ls.customer_name,
        ls.edition,
        ls.account_sfid,
        ls.opportunity_sfid,
        ls.stripeid,
        ls.issued_date,
        sub.status,
        sub.sfdc_migrated_opportunity_sfid
   from analytics.blp.license_server_fact ls
   left join  analytics.mattermost.excludable_servers es  on ls.server_id = es.server_id
   left join analytics.stripe.subscriptions sub on sub.license_id = ls.license_id 
        where ls.issued_date is not null
        and ls.edition in ('Cloud Professional')
        and es.reason is null
)

--cte to limit report to paying arr customers and exclude trial customers that did not convert to paying
,pop as (
    select
        opp.accountid
        ,sum(coalesce(opp.amount,0)) as arr
    from  {{ ref( 'opportunity') }} opp
    --from analytics.orgm.opportunity opp
    WHERE opp.iswon in (true,false)
        and opp.isclosed = true
        and opp.isdeleted = false
        and opp.license_key__c not in (select distinct license_id from mrr)
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
--modified to extend population to null values as a result of child ids created 
,ACCT AS (
    SELECT
      COALESCE(A.PARENTID,A.SFID) AS PARENT_ID
      ,A.SFID AS ACCOUNT_ID
      ,coalesce(parent_s_parent_acount__c,a.name) as parent_name
      ,A.GOVERNMENT__C AS GOVERNMENT
      ,coalesce(a.customer_segmentation_tier__c,max(A.CUSTOMER_SEGMENTATION_TIER__C) over (partition by parentid)) AS CUSTOMER_TIER
      ,coalesce(A.GEO__C,A.TERRITORY_GEO__C,max(a.geo__c) over (partition by parentid)) as geo
      ,A.BILLINGCOUNTRY AS COUNTRY
      ,A.COMPANY_TYPE__C AS COMPANY_TYPE
      ,A.HEALTH_SCORE__C AS HEALTH_SCORE
      ,coalesce(A.COSIZE__C,max(a.cosize__c) over (partition by parent_id)) AS COSIZE
      ,A.INDUSTRY
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
      ,opp.license_key__c as license_id
      ,opp.ownerid 
      --salesforce has null data bug with respect to license start and end
      --adding coalesce with stripe data to complement missing data
      ,coalesce(opp.license_start_date__c::date,s.current_period_start::date) as license_start
      --override for data input error
      ,case 
          when opp.id = '0063p00000zBnnuAAC' then date '2022-03-31' 
          else coalesce(opp.license_end_date__c::date,s.current_period_end::date) 
          end as license_end
      ,last_day(iff(opp.closedate>license_start,opp.closedate,license_start)::date) as report_month
      ,opp.closedate::date as close_date
      ,opp.license_active__c as license_active_sf
      --snowflake does not calculate date diff for months with decimals thus calculating on days 
      --common to see license periods adjusted for co terminating deals
      ,round(datediff('day',license_start,license_end)/(365/12),0) as term
      ,opp.iswon
      ,opp.type as opp_type
      ,case when opp.new_logo__c is null then 'No' else opp.new_logo__c end as new_logo
      ,round(coalesce(opp.amount,2),0) as bill_amt
      ,case when term = 12 then bill_amt 
            --else round(div0(bill_amt,term)*12,2) 
            else round(div0(bill_amt,datediff('day',license_start,license_end))*365,0)
            end as opportunity_arr
      ,opp.name as description
      --this field is not consistently populated and could be overwritten
      ,opp.original_opportunityid__c as original_id_renewed
      ,s.edition
    --from analytics.orgm.opportunity opp
    from {{ ref( 'opportunity') }} opp
    left join (select * from analytics.stripe.subscriptions where status = 'active') s 
        on s.license_id = opp.license_key__c
    --left join ANALYTICS.ORGM.ACCOUNT acct
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
--subquery for expiring licenses 

    select
      d.account_name
      ,d.account_id
      ,null as opportunity_id
      ,null as close_date
      ,dateadd('day',1,license_end) as lic_start_date
      ,dateadd('year',1,license_end) as lic_end_date
      ,12 as term
      ,false as iswon
      ,0 as bill_amt
      ,0 as opportunity_arr
      ,d.opportunity_arr *-1 as expired_arr
      ,0 as renewed_arr
      ,date_part('year',d.license_end)||'-'
        ||rank() over (partition by account_id, date_trunc('year',d.license_end)
        order by date_trunc('month',d.license_end)) 
        ||'-'||account_id 
        as match_key
      ,opportunity_id as original_id_renewed
    
    from d

union

--renewals won subquery
    select
      d.account_name
      ,d.account_id
      ,d.opportunity_id as opportunity_id
      ,close_date
      ,license_start as lic_start_date
      ,license_end as lic_end_date
      ,term
      ,iswon
      ,bill_amt
      ,opportunity_arr
      ,0 as expired_arr
      ,d.opportunity_arr as renewed_arr
      ,date_part('year',d.license_start)||'-'
        ||rank() over (partition by account_id, date_trunc('year',d.license_start) 
        order by date_trunc('month',d.license_start)) 
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
      ,sum(expired_arr) as expire_arr
      ,sum(renewed_arr) as renewal_arr
    from r
    group by 1,2,3,4

    union

    select
    account_name
      ,null as match_key
      ,account_id
      ,opportunity_id
      ,close_date
      ,license_start 
      ,license_end
      ,iswon as is_won
      ,opp_type 
      ,term
      ,bill_amt as billing_amt
      ,opportunity_arr 
      ,0 as expire_arr
      ,0 as renewal_arr
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

,final as (
--aggregating table to output
--separate subquery to avoid nested window functions    
  select
    master.account_name
    ,acct.parent_name
    ,master.account_id
    ,acct.parent_id
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
    ,master.account_id||'-'||trans_no as unique_key
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
    ,expire_arr
    ,renewal_arr
    ,l.description
    ,iff(description like '%inv:ONL%','SelfServeOnPrem','SalesServe') as product
    ,government
    ,customer_tier
    ,company_type
    ,cosize
    ,industry
    --,accountsource
    ,geo
    ,country
    --,type
    ,health_score 
    --below is the sales rep of the latest closed deal
    ,b.name as account_owner
    ,u.name as opportunity_owner
    ,current_date as date_refreshed
    ,l.edition
    from master
    left join 

      (select distinct opportunity_id, ownerid, close_date, license_start as start_date, license_end as end_date, license_active_sf, description, new_logo,edition from d) l 

      on l.opportunity_id = master.opportunity_id
    left join acct on acct.account_id = master.account_id
    left join b on b.accountid = master.account_id
    left join licterm on licterm.account_id = master.account_id
    left join analytics.orgm.user u on u.sfid=l.ownerid
    order by master.account_name,trans_no
)

select
    unique_key
    ,parent_name
    ,account_name
    ,parent_id
    ,account_id
    ,product
    --new field for looker
    ,coalesce(edition,split_part(description,'-',2)) as plan
    ,opportunity_id
    ,fiscal_year
    ,fiscal_quarter
    ,report_month
    ,report_date
    --new field for looker
    ,date_trunc('week',report_date)+4 as report_week
    ,closing_date
    ,license_start_date
    ,license_end_date
    ,newlogo
    ,trans_no
    ,opp_type
    ,term_months
    ,billing_amt
    ,iff(term_months<=12,billing_amt,round(div0(billing_amt,term_months)*12,2)) as first_yr_bill
    ,opportunity_arr
    ,expire_arr
    ,iff(trans_no = 1, 0,renewal_arr) as renew_arr
    ,case when report_month > last_day(current_date) then 0 else
        opportunity_arr + expire_arr end as arr_change
    ,sum(arr_change) over (partition by account_id order by trans_no) as ending_arr
    ,iff(trans_no = 1 and newlogo = 'Yes', opportunity_arr,0) as new_arr
    --new looker fields
    ,iff(arr_change>0,arr_change - new_arr,0) as expansion
    ,iff(arr_change<0,arr_change - new_arr,0) as expire_and_contract
    ,case when license_active_calc = true and is_won = true and report_month <= last_day(current_date)
        then opportunity_arr 
        else 0 
     end as active_arr
    ,description
    ,government
    ,customer_tier
    ,company_type
    ,cosize
    ,industry
    ,geo
    ,country
    ,health_score
    ,account_owner
    ,opportunity_owner
    ,account_start
    ,max_license
    ,beg_tenure_yr
    ,end_tenure_yr
    ,license_active_calc
    ,license_activesf
    ,status_aligned
    ,is_won
    ,date_refreshed
    --dormant fields to be removed from looker
    ,expire_and_contract as reduction_arr
    ,expansion as contract_expansion
    ,0 as account_expansion
    ,null as accountsource
    ,null as type
from final