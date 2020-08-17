UPDATE orgm.opportunity
    SET time_in_stage__c = current_date - opportunity.stage_change_date__c
WHERE opportunity.status_wlo__c = 'Open' AND time_in_stage__c IS DISTINCT FROM (current_date - opportunity.stage_change_date__c);

UPDATE orgm.opportunity
    SET time_in_stage__c = null
WHERE opportunity.status_wlo__c != 'Open' AND time_in_stage__c != null;