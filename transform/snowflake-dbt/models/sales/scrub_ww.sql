{{config({
    "materialized": 'table',
    "schema": "sales",
    "database": "DEV"
  })
}}

WITH ww_nn_amounts AS (
    SELECT 
        util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
        SUM(CASE WHEN NOT opportunity.status_wlo__c = 'Open' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c) ELSE 0 END) AS nn_open_max,
        SUM(CASE WHEN NOT opportunity.status_wlo__c = 'Open' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c) * probability * .01 ELSE 0 END) AS nn_open_weighted,
        SUM(CASE WHEN forecastcategoryname = 'Commit' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c) ELSE 0 END) AS nn_commit_max,
        SUM(CASE WHEN forecastcategoryname = 'Best Case' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c) ELSE 0 END) AS nn_best_case_max,
        SUM(CASE WHEN forecastcategoryname = 'Pipeline' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c) ELSE 0 END) AS nn_pipeline_max,
        SUM(CASE WHEN forecastcategoryname = 'Omitted' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c) ELSE 0 END) AS nn_omitted_max
    FROM {{ source('orgm','opportunity') }}
    LEFT JOIN {{ source('orgm','opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy')
    GROUP BY 1
), ww_ren_amounts AS (
    SELECT
        util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
        SUM(CASE WHEN NOT opportunity.status_wlo__c = 'Open' THEN renewal_amount__c ELSE 0 END) AS ren_open_max,
        SUM(CASE WHEN NOT opportunity.status_wlo__c = 'Open' THEN renewal_amount__c * probability * .01 ELSE 0 END) AS ren_open_weighted,
        SUM(CASE WHEN forecastcategoryname = 'Commit' THEN renewal_amount__c ELSE 0 END) AS ren_commit_max,
        SUM(CASE WHEN forecastcategoryname = 'Best Case' THEN renewal_amount__c ELSE 0 END) AS ren_best_case_max,
        SUM(CASE WHEN forecastcategoryname = 'Pipeline' THEN renewal_amount__c ELSE 0 END) AS ren_pipeline_max,
        SUM(CASE WHEN forecastcategoryname = 'Omitted' THEN renewal_amount__c ELSE 0 END) AS ren_omitted_max
    FROM {{ source('orgm','opportunity') }}
    LEFT JOIN {{ source('orgm','opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy')
    GROUP BY 1
), ww_ren_prev_amounts AS (
    SELECT 
        util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
        SUM(CASE WHEN opportunity.forecastcategoryname = 'Omitted' AND original_opportunity.status_wlo__c = 'Won' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c + renewal_amount__c) ELSE 0 END) AS ren_omitted_orig_amount_max
    FROM {{ source('orgm','opportunity') }}
    LEFT JOIN {{ source('orgm','opportunity') }} AS original_opportunity ON (coalesce(opportunity.original_opportunity__c, opportunity.original_opportunity_id__c)) = original_opportunity.sfid
    LEFT JOIN {{ source('orgm','opportunitylineitem') }} AS original_opportunitylineitem ON original_opportunity.sfid = original_opportunitylineitem.opportunityid
    WHERE util.fiscal_year(opportunity.closedate) = util.get_sys_var('curr_fy')
    GROUP BY 1
), ww_available_renewals AS (
    SELECT
        license_end_qtr AS qtr,
        SUM(available_renewals) AS available_renewals,
        SUM(gross_renewal_amount) AS gross_renewal_amount,
        ROUND(SUM(gross_renewal_amount)/SUM(available_renewals),3) AS renewal_rate
    FROM {{ ref('account_renewal_rate_by_qtr') }}
    LEFT JOIN {{ source('orgm','account') }} ON account_renewal_rate_by_qtr.account_sfid = account.sfid
    GROUP BY 1
), scrub_ww AS (
    SELECT 
        tva_bookings_new_and_exp_by_qtr.qtr,
        commit_ww.commit_netnew AS nn_forecast,
        commit_ww.upside_netnew AS nn_upside,
        tva_bookings_new_and_exp_by_qtr.target AS nn_target,
        tva_bookings_new_and_exp_by_qtr.actual AS nn_actual,
        tva_bookings_new_and_exp_by_qtr.tva AS nn_tva,
        nn_open_max,
        nn_open_weighted,
        nn_commit_max,
        nn_best_case_max,
        nn_pipeline_max,
        nn_omitted_max,
        commit_ww.commit_renewal AS ren_forecast,
        commit_ww.upside_renewal AS ren_upside,
        tva_bookings_ren_by_qtr.target AS ren_target,
        tva_bookings_ren_by_qtr.actual AS ren_actual,
        tva_bookings_ren_by_qtr.tva AS ren_tva,
        ren_open_max,
        ren_open_weighted,
        ren_commit_max,
        ren_best_case_max,
        ren_pipeline_max,
        ren_omitted_max,
        ren_omitted_orig_amount_max,
        available_renewals AS ren_available,
        gross_renewal_amount AS ren_gross_amount,
        renewal_rate AS ren_rate
    FROM {{ ref('tva_bookings_new_and_exp_by_qtr') }}
    LEFT JOIN {{ ref('tva_bookings_ren_by_qtr') }} ON tva_bookings_new_and_exp_by_qtr.qtr = tva_bookings_ren_by_qtr.qtr 
    LEFT JOIN {{ source('sales','commit_ww') }} ON tva_bookings_new_and_exp_by_qtr.qtr = commit_ww.qtr 
    LEFT JOIN ww_nn_amounts ON ww_nn_amounts.qtr = tva_bookings_new_and_exp_by_qtr.qtr
    LEFT JOIN ww_ren_amounts ON ww_ren_amounts.qtr = tva_bookings_ren_by_qtr.qtr
    LEFT JOIN ww_ren_prev_amounts ON ww_ren_prev_amounts.qtr = tva_bookings_ren_by_qtr.qtr
    LEFT JOIN ww_available_renewals ON ww_available_renewals.qtr = tva_bookings_ren_by_qtr.qtr
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27
)

SELECT * FROM scrub_ww