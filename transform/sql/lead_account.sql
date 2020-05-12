UPDATE orgm.lead
SET existing_account__c = account.sfid
FROM orgm.account
WHERE account.cbit__clearbitdomain__c = split_part(email,'@',2)
    AND converteddate IS NULL 
  AND existing_account__c IS NULL