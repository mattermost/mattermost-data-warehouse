UPDATE orgm.account
    SET health_score__c = account_health_score.health_score_w_override
FROM staging.account_health_score
WHERE account.sfid = account_health_score.account_sfid
    AND (health_score__c <> account_health_score.health_score_w_override OR health_score__c IS NULL);

UPDATE orgm.account
    SET health_score__c = NULL
WHERE NOT EXISTS (SELECT 1 FROM staging.account_health_score WHERE account.sfid = account_health_score.account_sfid)
    AND (SELECT COUNT(*) FROM staging.account_health_score) > 500
    AND health_score__c IS NOT NULL;