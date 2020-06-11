{{config({
    "materialized": 'table',
    "schema": "staging"
  })
}}

WITH tva_attain_new_and_exp_by_segment_by_fy AS (
    SELECT
        REPLACE(target_slug,'_mo_','_qtr_') AS target_slug,
        util.fiscal_year(tva_attain_new_and_exp_by_segment_by_mo.month) AS fy,
        min(period_first_day) AS period_first_day,
        max(period_last_day) AS period_last_day,
        sum(tva_attain_new_and_exp_by_segment_by_mo.target) AS target,
        sum(tva_attain_new_and_exp_by_segment_by_mo.actual) AS actual,
        round(sum(tva_attain_new_and_exp_by_segment_by_mo.actual)/sum(tva_attain_new_and_exp_by_segment_by_mo.target),3) AS tva
    FROM {{ ref('tva_attain_new_and_exp_by_segment_by_mo') }}
    GROUP BY 1,2
)

SELECT * FROM tva_attain_new_and_exp_by_segment_by_fy