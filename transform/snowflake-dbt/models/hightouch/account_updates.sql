{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with orgm_account_telemetry as (
    select
        account_sfid,
        MAX(enterprise_license_fact.last_license_telemetry_date) as last_telemetry_date,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_dau,0)) as dau,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_mau,0)) as mau,
        MAX(enterprise_license_fact.current_license_server_version) as server_version,
        SUM(COALESCE(current_max_license_registered_users,0)) as registered_users
    from {{ ref('enterprise_license_fact') }}
    join {{ ref('account') }} on account.sfid = enterprise_license_fact.account_sfid
    where enterprise_license_fact.last_license_telemetry_date is not null
    group by 1
), account_type as (
    select 
        account.sfid,
        COALESCE(account.arr_current__c,0) as account_arr, 
        SUM(COALESCE(child.arr_current__c,0)) as child_arr,
        COALESCE(account.seats_licensed__c,0) as account_seats, 
        SUM(COALESCE(child.seats_licensed__c,0)) as child_seats
    from {{ ref('account') }}
    left join {{ ref('account') }} as child on child.parentid = account.sfid
    group by 1, 2, 4
), account_updates as (
    select 
        account.sfid,
        case 
            when account.type not in ('Vendor','Partner') and (account_type.account_arr > 0 or account_type.child_arr > 0 or account_type.account_seats > 0 OR account_type.child_seats > 0) then 'Customer'
            when account.type = 'Customer' then 'Customer (Attrited)'
            else account.type
        end as account_type,
        coalesce(account_arr_and_seats.total_arr, account.arr_current__c) as total_arr, 
        coalesce(account_arr_and_seats.seats, account.seats_licensed__c) as seats,
        coalesce(orgm_account_telemetry.last_telemetry_date, account.latest_telemetry_date__c::date) as last_telemetry_date,
        CASE WHEN orgm_account_telemetry.dau > COALESCE(account.seats_active_max__c,0) THEN orgm_account_telemetry.dau ELSE account.seats_active_max__c END as seats_active_max,
        coalesce(orgm_account_telemetry.dau, account.seats_active_latest__c) as dau,
        coalesce(orgm_account_telemetry.mau, account.seats_active_mau__c) as mau,
        coalesce(orgm_account_telemetry.server_version, account.server_version__c) as server_version,
        coalesce(orgm_account_telemetry.registered_users, account.seats_registered__c) as registered_users
    from {{ ref('account') }}
    left join {{ ref('account_arr_and_seats') }} on account.sfid = account_arr_and_seats.account_sfid
    left join {{ ref('account_health_score') }} on account.sfid = account_health_score.account_sfid
    left join account_type on account.sfid = account_type.sfid
    left join orgm_account_telemetry on account.sfid = orgm_account_telemetry.account_sfid and not account.seats_active_override__c
    where 
        (account.arr_current__c, account.seats_licensed__c, account.health_score__c, account.type, account.latest_telemetry_date__c::date, account.seats_active_latest__c, account.seats_active_mau__c, account.server_version__c, account.seats_registered__c)
        is distinct from
        (account_arr_and_seats.total_arr, account_arr_and_seats.seats, account_health_score.health_score_w_override, account_type, orgm_account_telemetry.last_telemetry_date, orgm_account_telemetry.dau, orgm_account_telemetry.mau, orgm_account_telemetry.server_version, orgm_account_telemetry.registered_users)
)

select * from account_updates