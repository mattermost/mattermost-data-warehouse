UPDATE orgm.opportunitylineitem
    SET new_amount__c = oli_totals.total_new_amount,
        renewal_amount__c = oli_totals.total_renewal_amount,
        expansion_amount__c = oli_totals.total_expansion_amount,
        coterm_expansion_amount__c = oli_totals.total_coterm_amount,
        leftover_expansion_amount__c = oli_totals.total_leftover_amount,
        multi_amount__c = oli_totals.total_multi_amount
FROM (
        SELECT
            opportunitylineitem.sfid as oli_sfid,
            CASE WHEN opportunitylineitem.product_line_type__c = 'New' THEN opportunitylineitem.totalprice ELSE 0 END AS total_new_amount,
            CASE WHEN opportunitylineitem.product_line_type__c = 'Ren' THEN opportunitylineitem.totalprice ELSE 0 END AS total_renewal_amount,
            CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' AND opportunitylineitem.is_prorated_expansion__c IS NULL THEN opportunitylineitem.totalprice ELSE 0 END AS total_expansion_amount,
            CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' AND opportunitylineitem.is_prorated_expansion__c = 'Co-Termed Expansion' THEN opportunitylineitem.totalprice ELSE 0 END AS total_coterm_amount,
            CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' AND opportunitylineitem.is_prorated_expansion__c = 'Leftover Expansion' THEN opportunitylineitem.totalprice ELSE 0 END AS total_leftover_amount,
            CASE WHEN opportunitylineitem.product_line_type__c = 'Multi' THEN opportunitylineitem.totalprice ELSE 0 END AS total_multi_amount
        FROM orgm.opportunitylineitem
    ) as oli_totals
WHERE oli_totals.oli_sfid = opportunitylineitem.sfid AND opportunitylineitem.product_line_type__c != 'Self-Service' AND opportunitylineitem.amount_manual_override__c
    AND (new_amount__c, renewal_amount__c, expansion_amount__c, coterm_expansion_amount__c, leftover_expansion_amount__c, multi_amount__c) IS DISTINCT FROM
        (oli_totals.total_new_amount, oli_totals.total_renewal_amount, oli_totals.total_expansion_amount, oli_totals.total_coterm_amount, oli_totals.total_leftover_amount, oli_totals.total_multi_amount);
