WITH leap_years AS (
    SELECT
        date_day as date
    FROM {{ ref('arr_days') }}
    WHERE date_day::varchar LIKE '%-02-29'
    GROUP BY 1
), opportunitylineitems_impacted AS (
    SELECT
        o.opportunity_id,
        oli.opportunity_line_item_id,
        MAX(CASE WHEN leap_years.date BETWEEN start_date__c::date AND end_date__c::date THEN 1 ELSE 0 END) AS crosses_leap_day
    FROM
        {{ ref('stg_salesforce__opportunity') }} o
        LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} oli ON o.opportunity_id = oli.opportunity_id
        LEFT JOIN leap_years ON 1 = 1
    GROUP BY o.opportunity_id, oli.opportunity_line_item_id
), account_w_arr AS (
    SELECT
        a.account_id AS account_id,
        SUM(365*(oli.total_price)/(datediff(day, oli.start_date__c::date, oli.end_date__c::date) +  + 1 - crosses_leap_day)) AS total_arr
    FROM
        {{ ref('stg_salesforce__opportunity_line_item') }} oli
        LEFT JOIN opportunitylineitems_impacted ON opportunitylineitems_impacted.opportunity_line_item_id = oli.opportunity_line_item_id
        LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON o.opportunity_id = oli.opportunity_id
        LEFT JOIN {{ ref('stg_salesforce__account') }} a ON a.account_id = o.account_id
    WHERE
        o.is_won
        AND oli.end_date__c::date - oli.start_date__c::date <> 0
        AND current_date >= oli.start_date__c::date
        AND current_date <= oli.end_date__c::date
    GROUP BY 1
), seats_licensed AS (
    SELECT
        a.account_id AS account_id,
        SUM(CASE WHEN product2.family = 'License' THEN oli.quantity ELSE 0 END) AS seats
    FROM
        {{ ref('stg_salesforce__account') }} a
        LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON a.account_id = o.account_id AND o.status_wlo__c = 'Won'
        LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} oli ON o.opportunity_id = oli.opportunity_id
        LEFT JOIN {{ ref('stg_salesforce__product2') }} product2 ON oli.product2_id = product2.product2_id
    WHERE
        current_date >= oli.start_date__c AND current_date <= oli.end_date__c
    GROUP BY 1
    HAVING SUM(CASE WHEN product2.family = 'License' THEN oli.quantity ELSE 0 END) > 0
)
SELECT
    a.account_id AS account_id,
    GREATEST(COALESCE(total_arr, 0),0) AS total_arr,
    GREATEST(COALESCE(seats, 0),0) AS seats
FROM
    {{ ref('stg_salesforce__account') }} a
    LEFT JOIN account_w_arr ON a.account_id = account_w_arr.account_id
    LEFT JOIN seats_licensed ON a.account_id = seats_licensed.account_id
