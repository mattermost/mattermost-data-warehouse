BEGIN;

UPDATE orgm.account
SET type = 'Customer (Attrited)'
WHERE COALESCE(arr_current__c,0) = 0
    AND type = 'Customer';

UPDATE orgm.account
SET type = 'Customer'
WHERE arr_current__c > 0
    AND type IS DISTINCT FROM 'Customer';

COMMIT;