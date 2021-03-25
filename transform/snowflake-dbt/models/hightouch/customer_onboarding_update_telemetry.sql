{{config({
    "materialized": 'table',
    "schema": "hightouch"
  })
}}

with customer_onboarding_update_telemetry as (
    select
        sfid,
        licenseid,
        MAX(enterprise_license_fact.last_license_telemetry_date) AS last_telemetry_date,
        MAX(CASE WHEN COALESCE(enterprise_license_fact.current_max_license_server_dau,0) > COALESCE(customer_onboarding__c.seats_active_max__c,0) THEN enterprise_license_fact.current_max_license_server_dau ELSE customer_onboarding__c.seats_active_max__c END) as seats_active_max,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_dau,0)) AS dau,
        SUM(COALESCE(enterprise_license_fact.current_max_license_server_mau,0)) AS mau,
        MAX(enterprise_license_fact.current_license_server_version) AS server_version,
        SUM(COALESCE(current_max_license_registered_users,0)) AS registered_users
    from {{ ref('customer_onboarding__c') }}
    join {{ ref('enterprise_license_fact') }} on licenseid = customer_onboarding__c.license_key__c and not customer_onboarding__c.seats_active_override__c
    group by 1, 2
)

select customer_onboarding_update_telemetry.* 
from customer_onboarding_update_telemetry
join {{ ref('customer_onboarding__c') }} on customer_onboarding__c.sfid = customer_onboarding_update_telemetry.sfid
where
    (customer_onboarding_update_telemetry.dau,
    customer_onboarding_update_telemetry.mau,
    customer_onboarding_update_telemetry.last_telemetry_date,
    customer_onboarding_update_telemetry.server_version,
    customer_onboarding_update_telemetry.registered_users)
    is distinct from
    (customer_onboarding__c.seats_active_latest__c,
    customer_onboarding__c.seats_active_mau__c,
    customer_onboarding__c.latest_telemetry_date__c::date,
    customer_onboarding__c.server_version__c,
    customer_onboarding__c.seats_registered__c)
