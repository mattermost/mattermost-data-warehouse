WITH  account_w_arr AS (
    SELECT
        a.account_id AS account_id,
        SUM(365*(oli.totalprice)/datediff(day, oli.end_date__c::date,  oli.start_date__c::date)) AS total_arr
    FROM {{ ref('stg_salesforce__opportunity_line_item') }} oli
    LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON o.sfid = oli.opportunity_id
    LEFT JOIN {{ ref('stg_salesforce__account') }} a ON a.account_id = o.account_id
    WHERE o.is_won
        AND o.end_date__c::date <> o.start_date__c::date
        AND current_date >= oli.start_date__c::date
        AND current_date <= oli.end_date__c::date
    GROUP BY 1
)
SELECT
    a.account_id AS account_id,
    GREATEST(COALESCE(arr.total_arr, 0),0) AS total_arr
FROM {{ ref('stg_salesforce__account') }} a
LEFT JOIN account_w_arr arr ON a.sfid = account_w_arr.account_sfid
