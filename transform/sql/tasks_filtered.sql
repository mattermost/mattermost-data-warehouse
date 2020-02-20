BEGIN;

INSERT INTO staging.tasks_filtered
SELECT
    priority
    ,completeddatetime
    ,accountid
    ,calldurationinseconds
    ,subject
    ,lastmodifieddate
    ,ownerid
    ,isdeleted
    ,systemmodstamp
    ,ishighpriority
    ,lastmodifiedbyid
    ,status
    ,tasksubtype
    ,createddate
    ,isclosed
    ,calltype
    ,calldisposition
    ,createdbyid
    ,type
    ,description
    ,callobject
    ,activitydate
    ,dwh_external_id__c
    ,sfid
    ,now()
FROM orgm.task
WHERE task.type IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM staging.tasks_filtered WHERE tasks_filtered.sfid = task.sfid);

UPDATE staging.tasks_filtered
SET priority = task.priority,
    completeddatetime = task.completeddatetime,
    accountid = task.accountid,
    calldurationinseconds = task.calldurationinseconds,
    subject = task.subject,
    lastmodifieddate = task.lastmodifieddate,
    ownerid = task.ownerid,
    isdeleted = task.isdeleted,
    systemmodstamp = task.systemmodstamp,
    ishighpriority = task.ishighpriority,
    lastmodifiedbyid = task.lastmodifiedbyid,
    status = task.status,
    tasksubtype = task.tasksubtype,
    createddate = task.createddate,
    isclosed = task.isclosed,
    calltype = task.calltype,
    calldisposition = task.calldisposition,
    createdbyid = task.createdbyid,
    type = task.type,
    description = task.description,
    callobject = task.callobject,
    activitydate = task.activitydate,
    dwh_external_id__c = task.dwh_external_id__c,
    sfid = task.sfid,
    updated_at = now()
FROM orgm.task
WHERE tasks_filtered.systemmodstamp != task.systemmodstamp AND tasks_filtered.sfid = task.sfid;

COMMIT;