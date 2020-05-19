DELETE FROM staging.opportunity_snapshot WHERE snapshot_date = current_date;

INSERT INTO staging.opportunity_snapshot
SELECT
    current_date AS snapshot_date,
    sfid,
    name, 
    ownerid, 
    type, 
    closedate,
    iswon,
    isclosed,
    probability,
    stagename,
    forecastcategoryname, 
    expectedrevenue, 
    amount
FROM orgm.opportunity;

