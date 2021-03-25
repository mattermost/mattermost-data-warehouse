{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with account_type as (
    select 
        account.sfid,
        COALESCE(account.arr_current__c,0) as account_arr, 
        SUM(COALESCE(child.arr_current__c,0)) as child_arr,
        COALESCE(account.seats_licensed__c,0) as account_seats, 
        SUM(COALESCE(child.seats_licensed__c,0)) as child_seats
    from {{ ref('account') }}
    left join {{ ref('account') }} as child on child.parentid = account.sfid
    group by 1, 2, 4
), account_update_arr_seats_type as (
    select 
        account.sfid,
        account_arr_and_seats.total_arr as total_arr, 
        account_arr_and_seats.seats as seats,
        case 
            when account.type not in ('Vendor','Partner') and (account_type.account_arr > 0 or account_type.child_arr > 0 or account_type.account_seats > 0 OR account_type.child_seats > 0) then 'Customer'
            when account.type = 'Customer' then 'Customer (Attrited)'
            else account.type
        end as account_type
    from {{ ref('account') }}
    left join {{ ref('account_arr_and_seats') }} on account.sfid = account_arr_and_seats.account_sfid
    left join account_type on account.sfid = account_type.sfid
)

select account_update_arr_seats_type.*
from account_update_arr_seats_type
join {{ ref('account') }} on account.sfid = account_update_arr_seats_type.sfid
where 
    (round(account.arr_current__c,2), 
    account.seats_licensed__c, 
    account.type)
    is distinct from
    (round(account_update_arr_seats_type.total_arr,2), 
    account_update_arr_seats_type.seats, 
    account_update_arr_seats_type.account_type)