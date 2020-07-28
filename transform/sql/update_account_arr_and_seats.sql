UPDATE orgm.account
    SET arr_current__c = orgm_account_arr_and_seats.total_arr,
        seats_licenses__c = orgm_account_arr_and_seats.seats
FROM staging.orgm_account_arr_and_seats
WHERE orgm_account_arr_and_seats.account_sfid = account.sfid
    AND (orgm_account_arr_and_seats.total_arr,orgm_account_arr_and_seats.seats) IS DISTINCT FROM (account.arr_current__c,account.seats_licenses__c);