{{config({
    "materialized": 'table',
    "schema": "cs"
  })
}}

WITH actual_ren_by_qtr AS (
    SELECT
      util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
      ROUND(SUM(renewal_amount__c)) AS actual
    FROM {{ ref( 'opportunity') }} AS opportunity
    LEFT JOIN {{ ref( 'opportunitylineitem') }} AS opportunitylineitem ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE iswon
    GROUP BY 1
), tva_ren_by_qtr AS (
    SELECT
        renewal_by_qtr.qtr,
        util.fiscal_quarter_start(renewal_by_qtr.qtr) AS  period_first_day,
        util.fiscal_quarter_end(renewal_by_qtr.qtr) AS  period_last_day,
        renewal_by_qtr.target,
        actual_ren_by_qtr.actual,
        round((actual_ren_by_qtr.actual/renewal_by_qtr.target),3) AS tva
    FROM {{ source('sales_and_cs_gsheets', 'renewal_by_qtr') }}
    LEFT JOIN actual_ren_by_qtr ON renewal_by_qtr.qtr = actual_ren_by_qtr.qtr
)

SELECT * FROM tva_ren_by_qtr