UPDATE orgm.lead
SET never_connected__c = now(),
    status = 'Recycled',
    recycle_reason__c = 'Never Connected'
WHERE NOT actively_being_sequenced__c
    AND lead.status = 'SCL'
    AND lead.lead_status_minor__c = 'Outreach (Automation)';
