delete from staging.opportunity_snapshot where snapshot_date = to_char(now(),'YYYY-MM-DD');

Insert into staging.opportunity_snapshot
select 
    to_char(now(),'YYYY-MM-DD') as snapshot_date, sfid,
    name, ownerid, type, closedate, probability, stagename,
    forecastcategoryname, expectedrevenue, amount, arr__c
FROM orgm.opportunity
where to_char(closedate,'YYYY-MM') >= to_char(now(),'YYYY-MM');


