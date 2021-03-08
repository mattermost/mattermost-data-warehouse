{{config({
    "materialized": 'table',
    "schema": "cs"
  })
}}

WITH actual_ren_by_segment_by_qtr AS (
    SELECT
	    REPLACE(opportunity.territory_segment__c,'_','/') AS segment,
	    util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
	    ROUND(SUM(renewal_amount__c),2) AS actual
    FROM {{ ref('account') }}
        LEFT JOIN {{ ref('opportunity') }} ON account.sfid = opportunity.accountid
        LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy') AND opportunity.iswon
    GROUP BY 1,2
), tva_ren_by_segment_by_qtr AS (
    SELECT
        renewal_by_segment_by_qtr.segment,
        renewal_by_segment_by_qtr.qtr,
        util.fiscal_quarter_start(renewal_by_segment_by_qtr.qtr) AS  period_first_day,
        util.fiscal_quarter_end(renewal_by_segment_by_qtr.qtr) AS  period_last_day,
        renewal_by_segment_by_qtr.target,
        COALESCE(actual_ren_by_segment_by_qtr.actual,0) AS actual,
        ROUND(COALESCE(actual_ren_by_segment_by_qtr.actual,0)/renewal_by_segment_by_qtr.target,3) AS tva
    FROM {{ source('sales_and_cs_gsheets', 'renewal_by_segment_by_qtr') }}
    LEFT JOIN actual_ren_by_segment_by_qtr 
        ON renewal_by_segment_by_qtr.segment = actual_ren_by_segment_by_qtr.segment
            AND renewal_by_segment_by_qtr.qtr = actual_ren_by_segment_by_qtr.qtr
)

SELECT * FROM tva_ren_by_segment_by_qtr