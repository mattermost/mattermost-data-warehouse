UPDATE orgm.task
SET outreach_auto_reply_status__c = 'Not Outreach Reply'
WHERE subject NOT LIKE '[Outreach] [Email] [In] %'
    AND outreach_auto_reply_status__c IS NULL;

UPDATE orgm.task
SET outreach_auto_reply_status__c = 'Auto-reply'
WHERE subject LIKE '[Outreach] [Email] [In] %'
    AND subject LIKE ANY (SELECT concat('%',phrase,'%') FROM util.auto_reply_ooo_phrases)
    AND outreach_auto_reply_status__c IS NULL;

UPDATE orgm.lead
SET lead_status_minor__c = 'Connected'
FROM (
         SELECT whoid
         FROM orgm.task
         WHERE task.outreach_auto_reply_status__c IS NULL
         GROUP BY 1
     ) AS task_limited
WHERE lead.sfid = task_limited.whoid
  AND lead.actively_being_sequenced__c
  AND lead.status = 'SCL'
  AND lead.lead_status_minor__c = 'Outreach';

UPDATE orgm.task
SET outreach_auto_reply_status__c = 'Not Auto-reply'
WHERE subject LIKE '[Outreach] [Email] [In] %'
    AND subject NOT LIKE ANY (SELECT concat('%',phrase,'%') FROM util.auto_reply_ooo_phrases)
    AND outreach_auto_reply_status__c IS NULL;
