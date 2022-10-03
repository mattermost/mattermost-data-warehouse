{{config({
    "materialized": "table",
    "schema": "finance",
    "tags":'nightly'
  })
}}


--structure of joins is sales order have sales order lines that are united by transaction.id and transactionline.transaction
--sales order joined with invoice on salesforce account_id which is the opportunity id closed won
--entity_no relates to customer table via cusotmer.id
--getting opportunity id will only capture sales serve celigo sfnc salesforce id will be null for stripe
--for stripe transactions transactionid will be the stripe invoice id
--null opportunity id in netsuite for sales serve can suggest professional services denoted by class field professional services
--under transactionline department lists the p&l line item

with b as (
select
    inv.postingperiod as period_no
    ,inv.trandate::date as invoice_date
    ,iff(inv.source is null or inv.source = 'CSV','SalesServe',inv.source) as product
    ,inv.custbody_mm_online_order as online
    ,inv.tranid as invoice_id
    ,inv.id as transaction_id
    ,iff(inv.externalid = inv.tranid,null,inv.externalid) as stripe_inv_id
    ,inv.foreigntotal
    ,inv.exchangerate as fxrate
    ,inv.foreigntotal * fxrate as usd_amount
    ,inv.daysopen as days_open
    ,inv.daysoverduesearch as days_overdue
    ,inv.duedate::date as due_date
    ,inv.custbody_invoice_sent as invoice_sent
    ,inv.shippingaddress
     ,inv.custbody_suitesync_authorization_code as stripe_charge_id
    --link to invoice order lines is opportunity_id
    ,inv.custbody_celigo_sfnc_salesforce_id as opportunity_id
    --custbody_celigo_sfnc_sf_order_id,
    --link to transaction lines 
    ,so.id as so_id
    ,inv.email
    ,initcap(split_part(split_part(inv.email,'@',2),'.',1)) as domain
    ,inv.entity as entity_no
    ,inv.currency as fx_currency_code
    ,coalesce(inv.custbody5,so.custbody5) as opportunity_owner
    ,coalesce(inv.custbody6,so.custbody6) as csm_owner
from (
        select 
        * 
        --from analytics.netsuite_analytics.transaction 
        from {{ source('netsuite_analytics','transaction') }} 
        where abbrevtype = 'INV'
      ) inv
    left join (
        select 
        * 
        --from analytics.netsuite_analytics.transaction 
        from {{ source('netsuite_analytics','transaction') }}
        where abbrevtype = 'SALESORD'
        ) so 
        on so.custbody_celigo_sfnc_salesforce_id = inv.custbody_celigo_sfnc_salesforce_id
where inv.void = 'F'
    and inv.voided = 'F'
)
    
    
select
    p.periodname
    ,p.enddate::date as report_month
    ,last_day(date_trunc('week',invoice_date),'week') as report_week
    ,last_day(dateadd('month',1,last_day(dateadd('month',2,date_trunc('quarter',dateadd('month',-1,p.enddate)))))) as fiscal_quarter
    ,last_day(dateadd('month',1,last_day(dateadd('month',11,date_trunc('year',dateadd('month',-1,p.enddate)))))) as fiscal_year   
    ,cust.companyname
    ,t.parent_name
    ,t.account_name as sfdc_name
    ,b.*
    ,t.account_id
    ,t.parent_id
    ,cust.defaultbillingaddress
from b
    left join 
        (
            select 
                id,
                altname,
                companyname,
                entityid,
                custentity_stripe_customerid as stripe_cust_id,
                custentity_celigo_sfnc_salesforce_id as opportunity_id,
                defaultbillingaddress,
                balancesearch
            --from analytics.netsuite_analytics.customer
            from {{ source('netsuite_analytics','customer') }}
        ) cust
        on cust.id = b.entity_no
    left join (
            select 
                distinct opportunity_id, 
                account_name, 
                account_id, 
                parent_id, 
                parent_name 
            --from analytics.finance.arr_transactions
            from {{ ref('arr_transactions') }}
           ) t
           on t.opportunity_id = b.opportunity_id
    left join (
            select distinct 
                id, 
                periodname, 
                enddate
            --from analytics.netsuite_analytics.accountingperiod
            from {{ source('netsuite_analytics','accountingperiod') }}
            ) p
           on p.id = b.period_no
   where report_month > date '2020-01-31'
order by invoice_date asc
