
with account_type as (
    select
        a.account_id,
        COALESCE(a.arr_current__c,0) as account_arr,
        SUM(COALESCE(child.arr_current__c,0)) as child_arr,
        COALESCE(a.seats_licensed__c,0) as account_seats,
        SUM(COALESCE(child.seats_licensed__c,0)) as child_seats
    from {{ ref('stg_salesforce__account') }} a
    left join {{ ref('stg_salesforce__account') }} as child on child.parent_id = a.account_id
    group by 1, 2, 4
), account_update_arr_seats_type as (
    select
        a.account_id,
        arr.total_arr as total_arr,
        arr.seats as seats,
        case
            when a.type not in ('Vendor','Partner') and (account_type.account_arr > 0 or account_type.child_arr > 0 or account_type.account_seats > 0 OR account_type.child_seats > 0) then 'Customer'
            when a.type = 'Customer' then 'Customer (Attrited)'
            else a.type
        end as account_type
    from {{ ref('stg_salesforce__account') }} a
    left join {{ ref('int_account_arr_seats') }} arr on a.account_id = arr.account_id
    left join account_type on a.account_id = account_type.account_id
)

select
    account_update_arr_seats_type.account_id as sfid,
    account_update_arr_seats_type.total_arr,
    account_update_arr_seats_type.seats,
    account_update_arr_seats_type.account_type
from account_update_arr_seats_type
join {{ ref('stg_salesforce__account') }} a on a.account_id = account_update_arr_seats_type.account_id
where
    (round(a.arr_current__c,2),
    a.seats_licensed__c,
    a.type)
    is distinct from
    (round(account_update_arr_seats_type.total_arr,2),
    account_update_arr_seats_type.seats,
    account_update_arr_seats_type.account_type)