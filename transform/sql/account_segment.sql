BEGIN;

UPDATE orgm.account
SET
  territory_segment__c = rep.sales_segment__c
FROM orgm.user AS rep
WHERE account.ownerid = rep.sfid AND account.territory_segment__c IS DISTINCT FROM rep.sales_segment__c;

COMMIT;