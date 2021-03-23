{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with account_updates as (
    select 
        account.sfid, 
        coalesce(orgm_account_arr_and_seats.total_arr, account.arr_current__c) as total_arr, 
        coalesce(orgm_account_arr_and_seats.seats, account.seats_licensed__c) as seats,
        coalesce(orgm_account_telemetry.last_telemetry_date, account.latest_telemetry_date__c::date) as last_telemetry_date, 
        coalesce(orgm_account_telemetry.dau, account.seats_active_latest__c) as dau,
        coalesce(orgm_account_telemetry.mau, account.seats_active_mau__c) as mau,
        coalesce(orgm_account_telemetry.server_version, account.server_version__c) as server_version,
        coalesce(orgm_account_telemetry.registered_users, account.seats_registered__c) as registered_users
    from {{ ref ('account')}}
    left join {{ ref('orgm_account_arr_and_seats')}} on account.sfid = orgm_account_arr_and_seats.account_sfid
    left join {{ ref('orgm_account_telemetry')}} on account.sfid = orgm_account_telemetry.account_sfid and not account.seats_active_override__c
    where 
        (account.arr_current__c, account.seats_licensed__c, account.latest_telemetry_date__c::date, account.seats_active_latest__c, account.seats_active_mau__c, account.server_version__c, account.seats_registered__c)
        is distinct from
        (orgm_account_arr_and_seats.total_arr, orgm_account_arr_and_seats.seats, orgm_account_telemetry.last_telemetry_date, orgm_account_telemetry.dau, orgm_account_telemetry.mau, orgm_account_telemetry.server_version, orgm_account_telemetry.registered_users)
)

select * from account_updates