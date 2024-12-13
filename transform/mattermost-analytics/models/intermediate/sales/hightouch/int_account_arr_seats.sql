{{config({
    "materialized": 'table',
  })
}}

WITH account_w_arr AS (
    SELECT
        a.account_id AS account_id,
        SUM(365*(oli.total_price)/(datediff(day, oli.end_date__c::date , oli.start_date__c::date ))) AS total_arr
    FROM {{ ref('stg_salesforce__opportunity_line_item') }} oli
    LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON o.opportunity_id = oli.opportunity_id
    LEFT JOIN {{ ref('stg_salesforce__account') }} a ON a.account_id = o.account_id
    WHERE
        opportunity.iswon
        AND oli.end_date__c::date - oli.start_date__c::date <> 0
        AND current_date >= oli.start_date__c::date
        AND current_date <= oli.end_date__c::date
    GROUP BY 1
), seats_licensed AS (
    SELECT account.sfid AS account_sfid,
        SUM(CASE WHEN product2.family = 'License' THEN opportunitylineitem.quantity ELSE 0 END) AS seats
    FROM {{ ref('stg_salesforce__account') }} a
    LEFT JOIN {{ ref('stg_salesforce__opportunity') }} o ON a.account_id = o.account_id AND o.status_wlo__c = 'Won'
    LEFT JOIN {{ ref('stg_salesforce__opportunity_line_item') }} oli ON o.opportunity_id = oli.opportunity_id
    LEFT JOIN {{ ref('stg_salesforce__product2') }} ON oli.product2id = product2.product2_id
    WHERE current_date >= oli.start_date__c AND current_date <= oli.end_date__c
    GROUP BY 1
    HAVING SUM(CASE WHEN product2.family = 'License' THEN opportunitylineitem.quantity ELSE 0 END) > 0
), account_arr_and_seats AS (
    SELECT
        account.account_id AS account_id,
        GREATEST(COALESCE(total_arr, 0),0) AS total_arr,
        GREATEST(COALESCE(seats, 0),0) AS seats
    FROM {{ ref('stg_salesforce__account') }}
    LEFT JOIN account_w_arr ON account.sfid = account_w_arr.account_sfid
    LEFT JOIN seats_licensed ON account.sfid = seats_licensed.account_sfid
)

SELECT * FROM account_arr_and_seats
