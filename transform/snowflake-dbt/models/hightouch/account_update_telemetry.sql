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
), account_update_telemetry as (
    select 
        account.sfid,
        account.seats_active_override__c,
        orgm_account_telemetry.last_telemetry_date as last_telemetry_date,
        CASE WHEN orgm_account_telemetry.dau > COALESCE(account.seats_active_max__c,0) THEN orgm_account_telemetry.dau ELSE account.seats_active_max__c END as seats_active_max,
        orgm_account_telemetry.dau as dau,
        orgm_account_telemetry.mau as mau,
        orgm_account_telemetry.server_version as server_version,
        orgm_account_telemetry.registered_users as registered_users
    from {{ ref('account') }}
    left join orgm_account_telemetry on account.sfid = orgm_account_telemetry.account_sfid
    where not account.seats_active_override__c and
        (account.latest_telemetry_date__c::date, account.seats_active_latest__c, account.seats_active_mau__c, account.server_version__c, account.seats_registered__c)
        is distinct from
        (orgm_account_telemetry.last_telemetry_date, orgm_account_telemetry.dau, orgm_account_telemetry.mau, orgm_account_telemetry.server_version, orgm_account_telemetry.registered_users)
)

select * from account_update_telemetry