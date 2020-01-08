delete from orgm.opportunity_snapshot where snapshot_date = to_char(now(),'YYYY-MM-DD');

Insert into orgm.opportunity_snapshot
select to_char(now(),'YYYY-MM-DD') as snapshot_date, sfid, name, ownerid, type, stagename, closedate, amount
FROM opportunity
where to_char(closedate,'YYYY-MM') >= to_char(now(),'YYYY-MM');
