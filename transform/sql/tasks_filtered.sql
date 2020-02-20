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
FROM orgm.task
WHERE task.type IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM staging.tasks_filtered WHERE tasks_filtered.sfid = task.sfid);

COMMIT;
