UPDATE orgm.account
SET type = 'Customer (Attrited)',
    seats_active_mau__c = NULL,
    seats_licensed__c = NULL,
    seats_active_override__c = FALSE,
    seats_active_latest__c = NULL,
    latest_telemetry_date__c = NULL
FROM (
    SELECT
        account.sfid,
        COALESCE(account.arr_current__c,0) AS account_arr,
        SUM(COALESCE(child.arr_current__c,0)) AS child_arr,
        COALESCE(account.seats_licensed__c,0) AS account_seats,
        SUM(COALESCE(child.seats_licensed__c,0)) AS child_seats
    FROM orgm.account
    LEFT JOIN orgm.account AS child ON child.parentid = account.sfid
    WHERE account.type = 'Customer'
    GROUP BY 1, 2, 4
) AS account_arr_details
WHERE account.sfid = account_arr_details.sfid
    AND account_arr_details.account_arr = 0
    AND account_arr_details.child_arr = 0
    AND account_arr_details.account_seats = 0
    AND account_arr_details.child_seats = 0
    AND type = 'Customer';


UPDATE orgm.account
SET type = 'Customer'
FROM (
    SELECT 
        account.sfid, 
        COALESCE(account.arr_current__c,0) AS account_arr, 
        SUM(COALESCE(child.arr_current__c,0)) AS child_arr,
        COALESCE(account.seats_licensed__c,0) AS account_seats, 
        SUM(COALESCE(child.seats_licensed__c,0)) AS child_seats
    FROM orgm.account
    LEFT JOIN orgm.account AS child ON child.parentid = account.sfid
    WHERE account.type IS DISTINCT FROM 'Customer'
    GROUP BY 1, 2, 4
) AS account_arr_details
WHERE account.sfid = account_arr_details.sfid
    AND (account_arr_details.account_arr != 0 OR account_arr_details.child_arr != 0 
        OR account_arr_details.account_seats != 0 OR account_arr_details.child_seats != 0)
    AND type IS DISTINCT FROM 'Customer';
