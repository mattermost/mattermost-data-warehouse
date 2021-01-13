UPDATE orgm.customer_onboarding__c
    SET     
        seats_active_latest__c = license_telemetry.dau,
        seats_active_mau__c = license_telemetry.mau,
        seats_active_max__c = CASE WHEN license_telemetry.dau > COALESCE(customer_onboarding__c.seats_active_max__c,0) THEN license_telemetry.dau ELSE account.seats_active_max__c END,
        latest_telemetry_date__c = license_telemetry.last_telemetry_date,
        server_version__c = license_telemetry.server_version,
        seats_registered__c = license_telemetry.registered_users
FROM staging.license_telemetry
LEFT JOIN orgm.opportunity ON opportunity.license_key__c = license_telemetry.licenseid
WHERE opportunity.sfid = customer_onboarding__c.opportunity_name
    AND customer_onboarding__c.seats_active_override__c = FALSE
    AND (license_telemetry.dau,license_telemetry.mau,license_telemetry.last_telemetry_date,license_telemetry.server_version,license_telemetry.registered_users) IS DISTINCT FROM 
        (customer_onboarding__c.seats_active_latest__c,customer_onboarding__c.seats_active_mau__c,customer_onboarding__c.latest_telemetry_date__c,customer_onboarding__c.server_version__c, customer_onboarding__c.seats_registered__c);