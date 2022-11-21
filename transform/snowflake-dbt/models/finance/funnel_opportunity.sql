{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}


WITH ACCT AS (
    SELECT
      COALESCE(A.PARENTID,A.SFID) AS PARENT_ID
      ,coalesce(p.name,a.name) as parent_name
      ,A.SFID AS ACCOUNT_ID
      ,a.name as account_name
      ,A.GOVERNMENT__C AS GOVERNMENT
      ,coalesce(a.customer_segmentation_tier__c,max(A.CUSTOMER_SEGMENTATION_TIER__C) over (partition by parentid)) AS CUSTOMER_TIER
      ,coalesce(A.GEO__C,A.TERRITORY_GEO__C,max(a.geo__c) over (partition by parentid)) as geo
      ,A.BILLINGCOUNTRY AS COUNTRY
      ,A.COMPANY_TYPE__C AS COMPANY_TYPE
      ,A.HEALTH_SCORE__C AS HEALTH_SCORE
      ,coalesce(A.COSIZE__C,max(a.cosize__c) over (partition by parent_id)) AS COSIZE
      ,A.INDUSTRY
    --FROM ANALYTICS.ORGM.ACCOUNT a
    FROM     {{ ref( 'account') }} A
    LEFT JOIN (
        SELECT 
            distinct sfid 
            ,name 
       --FROM ANALYTICS.ORGM.ACCOUNT 
        FROM {{ ref( 'account') }}  
        WHERE 
            parentid is null
            and isdeleted = FALSE 
        ) p on p.sfid = parentid
    WHERE  ISDELETED = FALSE
    ORDER BY PARENT_ID, account_id
)



SELECT
    opp.SFID as opportunity_id 
    ,CLOSEDATE::DATE AS close_date
    ,STAGE_CHANGE_DATE__C::date as stage_changed
    ,opp.CREATEDDATE::date as opp_created
    ,LEAD_CREATED_DATE__C::date as lead_created
    ,last_day(closedate::date,month) as close_month
    ,last_day(stage_changed,month) as stage_changed_month
    ,last_day(opp_created,month) as opp_created_month
    ,last_day(lead_created,month) as lead_month
    ,OPP.FORECASTCATEGORY
    ,STAGENAME
    ,PROBABILITY 
    ,a.account_name
    ,a.parent_name
    ,opp.CREATEDBYID 
    ,USER.NAME as creator_name
    ,sales.name as owner_name
    ,ce.name as ce_name
    ,csm.name as csm_name
    ,new_logo__c
    ,LEAD_TYPE__C
    --mql activity is null for 2022 
    --,mql_date__c::date as mql_date 
    --,MQL_REASON__C
    ,LICENSE_START_DATE__C::date as license_start
    ,LICENSE_END_DATE__C::date as license_end
    ,round(datediff('day',license_start_date__c, license_end_date__c)/10,0)*10/30 as term_mos
    ,coalesce(opp.amount,0) as booking
    ,coalesce(opp.expectedrevenue,0) as prob_weight_booking
    ,case 
        when opp.type in ('New Subscription','New Business') then coalesce(t.new_arr,0)
        else round(div0(coalesce(opp.amount,0),coalesce(term_mos,12))*12,2)
        end as arr
    ,round(coalesce(probability,0) * arr/100,2) as prob_weight_arr
    ,opp.NAME as opportunity_name
    --,active_licenses__c
    ,OPP_PRODUCTS__C
    ,ISCLOSED
    ,ISWON
    --this is the active number of seats for the opportunity
    --not continuously populated
    ,LICENSE_ACTIVE__C
    ,CASE 
        WHEN FORECASTCATEGORY = 'Closed' THEN 1
        WHEN FORECASTCATEGORY = 'BestCase' THEN 2
        WHEN FORECASTCATEGORY = 'Forecast' THEN 3
        WHEN FORECASTCATEGORY = 'Pipeline' THEN 4
        ELSE 5
        END AS CATEGORYORDER 
    ,opp.ACCOUNTID as account_id
    ,a.parent_id
    ,opp.ownerid as owner_id 
    --self serve enabled is not reliable as it is false for everybody
    --,SELF_SERVICE_ENABLED__C as self_serve_enabled
    --PRESENT FOR BOTH WON AND PROPOSALS but mostly won covering both new subscription types and renewal
    --occurs only for online order types
    ,opp.STRIPE_ID__C as stripe_id
    --27% filled
    ,CAMPAIGNID as campaign_id
    --9% filled
    ,LEADID__C as lead_id
    ,LEAD_SOURCE_TEXT__C as lead_category
    ,LEAD_SOURCE_DETAIL__C as lead_detail
    ,LEAD_SOURCE_DETAIL_UPON_CONVERSION__C as lead_upon_conversion
    ,case 
        when opp.type in ('New Subscription','New Business') then 'New'
        when opp.type in ('Contract Expansion', 'Account Expansion','Reduction') then 'Expansion'
        when opp.type in ('Renewal','Reduction') then 'Renewal'
        when opp.type in ('Monthly Billing') then 'Monthly Billing'
        else 'Undeclared'
        end as dealtype
    ,opp.GEO__C as geo
    ,a.company_type
    ,opp.ORDER_TYPE__C as order_type
    ,date_trunc(week,closedate)::date as close_week_beg
    ,last_day(close_week_beg,week) as close_week_end
    ,date_trunc(week,stage_changed)::date as stage_changed_week_beg
    ,last_day(stage_changed_week_beg,week) as stage_changed_week_end
    ,date_trunc(week,opp_created)::date as opp_created_week_beg
    ,last_day(opp_created,week) as opp_created_week_end
    ,date_trunc(week,lead_created)::date as lead_week_beg
    ,last_day(lead_week_beg,week) as lead_week_end
--from analytics.orgm.opportunity opp
from {{ ref( 'opportunity') }} opp
--left join analytics.finance.arr_transactions t 
left join {{ref('arr_transactions')}} t
    on t.opportunity_id = opp.sfid
left join acct a
    on a.account_id = opp.accountid 
--left join analytics.orgm.user user
left join {{ref('user')}} user
    on user.id = opp.createdbyid
--left join analytics.orgm.user sales 
left join {{ref('user')}} sales
    on sales.id = opp.ownerid 
--left join analytics.orgm.user ce
left join {{ref('user')}} ce
    on ce.id = opp.ce_owner__c
--left join analytics.orgm.user csm
left join {{ref('user')}} csm
    on csm.id = opp.csm_owner__c 
where 
    lower(opp.name) not like '%test%'
    and type != 'Monthly Billing'
    and opp.isdeleted = false
    and opp.closedate > dateadd(month,-24,current_date)
    and opp.closedate <= dateadd(month,6,current_date)
order by close_month, categoryorder asc, opp.stagename desc, opp.amount desc

