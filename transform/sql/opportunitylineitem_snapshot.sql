DELETE FROM staging.opportunitylineitem_snapshot WHERE snapshot_date = current_date;

INSERT INTO staging.opportunitylineitem_snapshot
SELECT
    current_date AS snapshot_date,
    sfid,
    name,
    opportunityid,
    product2id,
    product_type__c,
    description,
    listprice,
    unitprice,
    discount,
    quantity,
    totalprice,
    start_date__c,
    end_date__c
    arr_contributed__c,
    revenue_type__c,
    product_line_type__c
FROM orgm.opportunitylineitem;