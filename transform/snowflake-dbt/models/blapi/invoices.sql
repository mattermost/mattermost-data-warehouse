{{config({
    "materialized": "incremental",
    "schema": "blp",
    "unique_key":"id",
    "tags":["nightly","blapi"],
    "database":"DEV"
  })
}}

WITH forecasted_invoice AS (
    SELECT 
        u1.subscription_id
      , u2.max_date
      , MAX(u1.active_users) AS max_users_previous_day
    FROM {{ source('blapi', 'usage_events') }} u1
    JOIN 
    (
        SELECT 
            subscription_id
        , CASE WHEN max(timestamp::date) = CURRENT_DATE THEN MAX(timestamp::date) - INTERVAL '1 DAY'
            WHEN MAX(timestamp::date) = CURRENT_DATE - INTERVAL '1 DAY' THEN MAX(TIMESTAMP::DATE) 
            WHEN MAX(timestamp::date) > CURRENT_DATE THEN MAX(CURRENT_DATE - INTERVAL '1 DAY') 
            ELSE NULL END AS max_date
        FROM {{ source('blapi', 'usage_events') }}
        GROUP BY 1
    ) u2
        ON u1.subscription_id = u2.subscription_id
        AND u1.timestamp::date = u2.max_date
    GROUP BY 1, 2
),

invoices AS (
    SELECT
        *
      , COALESCE(
             -- RETRIEVE INVOICE SUBTOTAL FOR MONTH-TO-DATE USAGE BASED ON AVAILABLE FIELDS IN INVOICES TABLE
            ((i.total_user_months - i.free_user_months) * 10
                -- DIVIDE BY NUMBER OF DAYS IN MONTH
                / DATEDIFF(DAY, DATE_TRUNC('MONTH', CURRENT_DATE), LAST_DAY(CURRENT_DATE, MONTH) + INTERVAL '1 DAY'))
                -- MULTIPLY BY NUMBER OF COMPLETE DAYS IN MONTH
                *DATEDIFF(DAY, DATE_TRUNC('MONTH', CURRENT_DATE), CURRENT_DATE) 

             -- CALCULATE REMAINING MONTHS FORECASTED INVOICE USING LAST COMPLETE DAYS MAX ACTIVE USER COUNT RECORDED IN THE USAGE_EVENTS RELATION
                -- Only calculate forecasted remaining month invoice if last usage > 10 users else 0  
            + CASE WHEN fi.max_users_previous_day > 10 THEN ((fi.max_users_previous_day * 10/ DATEDIFF(DAY, DATE_TRUNC('MONTH', CURRENT_DATE), LAST_DAY(CURRENT_DATE, MONTH) + INTERVAL '1 DAY'))
                * datediff(DAY, CURRENT_DATE, LAST_DAY(current_date, MONTH) + INTERVAL '1 DAY'))
                ELSE 0 END, i.total
                ) AS forecasted_invoice_total
    FROM {{ source('blapi', 'invoices') }} i
    LEFT JOIN forecasted_invoice fi
        ON i.subscription_id = fi.subscription_id
        AND i.start_date::date = DATE_TRUNC('month', fi.max_date)
    {% if is_incremental() %}

    WHERE i.invoice_build_date >= (SELECT MAX(invoice_build_date) FROM {{ this }})

    {% endif %}
)

SELECT
    *
FROM invoices