{{config({
    "materialized": 'table',
    "schema": "sales"
  })
}}

WITH actual_new_and_exp_by_qtr AS (
    SELECT
	    util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
	    ROUND(SUM(new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c),2) AS actual
    FROM {{ ref('opportunity') }}
        LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy') AND opportunity.iswon
    GROUP BY 1
), tva_new_and_exp_by_qtr AS (
    SELECT
        new_and_expansion_by_qtr.qtr,
        util.fiscal_quarter_start(new_and_expansion_by_qtr.qtr) AS period_first_day,
        util.fiscal_quarter_end(new_and_expansion_by_qtr.qtr) AS period_last_day,
        new_and_expansion_by_qtr.target,
        COALESCE(actual_new_and_exp_by_qtr.actual,0) AS actual,
        ROUND(COALESCE(actual_new_and_exp_by_qtr.actual,0)/new_and_expansion_by_qtr.target,3) AS tva
    FROM {{ source('sales_and_cs_gsheets', 'new_and_expansion_by_qtr') }}
    LEFT JOIN actual_new_and_exp_by_qtr ON actual_new_and_exp_by_qtr.qtr = new_and_expansion_by_qtr.qtr
)

SELECT * FROM tva_new_and_exp_by_qtr