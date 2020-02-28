BEGIN;

UPDATE orgm.account
    SET health_score__c = account_health_score.health_score_w_override
FROM staging.account_health_score
WHERE account.sfid = account_health_score.account_sfid
    AND health_score__c <> account_health_score.health_score_w_override;

COMMIT;