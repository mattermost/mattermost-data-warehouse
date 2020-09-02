UPDATE orgm.account
    SET     
        seats_active_latest__c = orgm_account_telemetry.dau,
        seats_active_mau__c = orgm_account_telemetry.mau,
        seats_active_max__c = CASE WHEN orgm_account_telemetry.dau > COALESCE(account.seats_active_max__c,0) THEN orgm_account_telemetry.dau ELSE account.seats_active_max__c END,
        latest_telemetry_date__c = orgm_account_telemetry.last_telemetry_date,
        server_version__c = orgm_account_telemetry.server_version
FROM staging.orgm_account_telemetry
WHERE orgm_account_telemetry.account_sfid = account.sfid
    AND seats_active_override__c = FALSE
    AND (orgm_account_telemetry.dau,orgm_account_telemetry.mau,orgm_account_telemetry.last_telemetry_date,server_version) IS DISTINCT FROM 
        (account.seats_active_latest__c,account.seats_active_mau__c,account.latest_telemetry_date__c,server_version__c);