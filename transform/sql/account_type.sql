BEGIN;

UPDATE orgm.account
SET type = 'Customer (Attrited)'
FROM (
    SELECT account.sfid, COALESCE(account.arr_current__c,0) AS account_arr, SUM(COALESCE(child.arr_current__c,0)) AS child_arr
    FROM orgm.account
    LEFT JOIN orgm.account AS child ON child.parentid = account.sfid
    GROUP BY 1, 2
) as account_arr_details
WHERE account.sfid = account_arr_details.sfid
    AND account_arr_details.account_arr = 0
    AND account_arr_details.child_arr = 0
    AND type = 'Customer';

UPDATE orgm.account
SET type = 'Customer'
FROM (
    SELECT account.sfid, COALESCE(account.arr_current__c,0) AS account_arr, SUM(COALESCE(child.arr_current__c,0)) AS child_arr
    FROM orgm.account
    LEFT JOIN orgm.account AS child ON child.parentid = account.sfid
    GROUP BY 1, 2
) AS account_arr_details
WHERE account.sfid = account_arr_details.sfid
    AND (account_arr_details.account_arr != 0 OR account_arr_details.child_arr != 0)
    AND type IS DISTINCT FROM 'Customer';

COMMIT;