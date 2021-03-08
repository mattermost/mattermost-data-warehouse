{{config({
    "materialized": 'table',
    "schema": "sales"
  })
}}

WITH ww_nn_amounts AS (
    SELECT 
        util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) AS qtr,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Open' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) ELSE 0 END) AS nn_open_max,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Open' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) * probability * .01 ELSE 0 END) AS nn_open_weighted,
        SUM(CASE WHEN forecastcategoryname = 'Commit' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) ELSE 0 END) AS nn_commit_max,
        SUM(CASE WHEN forecastcategoryname = 'Best Case' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) ELSE 0 END) AS nn_best_case_max,
        SUM(CASE WHEN forecastcategoryname = 'Pipeline' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) ELSE 0 END) AS nn_pipeline_max,
        SUM(CASE WHEN forecastcategoryname = 'Omitted' THEN (new_amount__c + expansion_amount__c + coterm_expansion_amount__c + leftover_expansion_amount__c) ELSE 0 END) AS nn_omitted_max
    FROM {{ ref('opportunity') }}
    LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
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
    FROM {{ ref('opportunity') }}
    LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE util.fiscal_year(closedate) = util.get_sys_var('curr_fy')
    GROUP BY 1
), ww_available_renewals AS (
    SELECT
        util.fiscal_year(renewal_rate_by_renewal_opportunity.renewal_date)|| '-' || util.fiscal_quarter(renewal_rate_by_renewal_opportunity.renewal_date) AS qtr,
        SUM(renewal_rate_by_renewal_opportunity.available_renewal) AS available_renewals,
        SUM(CASE WHEN renewal_date < current_date THEN renewal_rate_by_renewal_opportunity.available_renewal ELSE 0 END) AS available_renewals_qtd,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) AS available_renewals_won,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Open' THEN renewal_rate_by_renewal_opportunity.open_renewal_gross_total ELSE 0 END) AS available_renewals_open,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Lost' THEN renewal_rate_by_renewal_opportunity.lost_renewal_gross_total ELSE 0 END) AS available_renewals_lost,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' AND renewal_date < current_date THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) AS available_renewals_won_qtd,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' AND renewal_date < current_date AND util.fiscal_year(renewal_rate_by_renewal_opportunity.renewal_date)|| '-' || util.fiscal_quarter(renewal_rate_by_renewal_opportunity.renewal_date) = util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END)  AS available_renewals_won_in_qtr_and_qtd,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Open' AND renewal_date < current_date THEN renewal_rate_by_renewal_opportunity.open_renewal_gross_total ELSE 0 END) AS available_renewals_open_past_due_qtd,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Lost' AND renewal_date < current_date THEN renewal_rate_by_renewal_opportunity.lost_renewal_gross_total ELSE 0 END) AS available_renewals_lost_qtd,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) / SUM(renewal_rate_by_renewal_opportunity.available_renewal) AS available_renewals_won_perc,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Open' THEN renewal_rate_by_renewal_opportunity.open_renewal_gross_total ELSE 0 END) / SUM(renewal_rate_by_renewal_opportunity.available_renewal) AS available_renewals_open_perc,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Lost' THEN renewal_rate_by_renewal_opportunity.lost_renewal_gross_total ELSE 0 END) / SUM(renewal_rate_by_renewal_opportunity.available_renewal) AS available_renewals_lost_perc,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' AND renewal_date < current_date THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) / NULLIF(SUM(CASE WHEN renewal_date < current_date THEN renewal_rate_by_renewal_opportunity.available_renewal ELSE 0 END),0) AS available_renewals_won_qtd_perc,
        (SUM(CASE WHEN opportunity.status_wlo__c = 'Won' THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) + SUM(CASE WHEN opportunity.status_wlo__c = 'Open' THEN renewal_rate_by_renewal_opportunity.available_renewal ELSE 0 END)) / SUM(renewal_rate_by_renewal_opportunity.available_renewal) AS available_renewals_won_max_perc,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' AND util.fiscal_year(renewal_rate_by_renewal_opportunity.renewal_date)|| '-' || util.fiscal_quarter(renewal_rate_by_renewal_opportunity.renewal_date) = util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) AS available_renewals_won_in_qtr,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' AND renewal_rate_by_renewal_opportunity.renewal_date < opportunity.closedate AND util.fiscal_year(renewal_rate_by_renewal_opportunity.renewal_date)|| '-' || util.fiscal_quarter(renewal_rate_by_renewal_opportunity.renewal_date) != util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) AS available_renewals_won_late,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Won' AND renewal_rate_by_renewal_opportunity.renewal_date > opportunity.closedate AND util.fiscal_year(renewal_rate_by_renewal_opportunity.renewal_date)|| '-' || util.fiscal_quarter(renewal_rate_by_renewal_opportunity.renewal_date) != util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) THEN renewal_rate_by_renewal_opportunity.won_renewal_gross_total ELSE 0 END) AS available_renewals_won_early,
        SUM(CASE WHEN opportunity.status_wlo__c = 'Open' AND util.fiscal_year(renewal_rate_by_renewal_opportunity.renewal_date)|| '-' || util.fiscal_quarter(renewal_rate_by_renewal_opportunity.renewal_date) = util.fiscal_year(opportunity.closedate)|| '-' || util.fiscal_quarter(opportunity.closedate) THEN renewal_rate_by_renewal_opportunity.open_renewal_gross_total ELSE 0 END) AS available_renewals_open_in_qtr
    FROM {{ ref('renewal_rate_by_renewal_opportunity') }}
    LEFT JOIN {{ ref('opportunity') }} ON opportunity.sfid = renewal_rate_by_renewal_opportunity.opportunityid
    GROUP BY 1
), scrub_ww AS (
    SELECT 
        tva_new_and_exp_by_qtr.qtr,
        forecast.commit_netnew AS nn_forecast,
        forecast.upside_netnew AS nn_upside,
        tva_new_and_exp_by_qtr.target AS nn_target,
        tva_new_and_exp_by_qtr.actual AS nn_actual,
        tva_new_and_exp_by_qtr.tva AS nn_tva,
        nn_open_max,
        nn_open_weighted,
        nn_commit_max,
        nn_best_case_max,
        nn_pipeline_max,
        nn_omitted_max,
        forecast.commit_renewal AS ren_forecast,
        forecast.upside_renewal AS ren_upside,
        tva_ren_by_qtr.target AS ren_target,
        tva_ren_by_qtr.actual AS ren_actual,
        tva_ren_by_qtr.tva AS ren_tva,
        ren_open_max,
        ren_open_weighted,
        ren_commit_max,
        ren_best_case_max,
        ren_pipeline_max,
        ren_omitted_max,
        available_renewals AS ren_available,
        available_renewals_qtd AS ren_available_renewals_qtd,
        available_renewals_won AS ren_available_renewals_won,
        available_renewals_open AS ren_available_renewals_open,
        available_renewals_lost AS ren_available_renewals_lost,
        available_renewals_won_perc AS ren_available_renewals_won_perc,
        available_renewals_open_perc AS ren_available_renewals_open_perc,
        available_renewals_lost_perc AS ren_available_renewals_lost_perc,
        available_renewals_won_qtd_perc AS ren_available_renewals_won_qtd_perc,
        available_renewals_won_max_perc AS ren_available_renewals_won_max_perc,
        available_renewals_won_in_qtr AS ren_available_renewals_won_in_qtr,
        available_renewals_won_early AS ren_available_renewals_won_early,
        available_renewals_won_late AS ren_available_renewals_won_late,
        available_renewals_open_in_qtr AS ren_available_renewals_open_in_qtr,
        available_renewals_won_qtd AS ren_available_renewals_won_qtd,
        available_renewals_won_in_qtr_and_qtd AS ren_available_renewals_won_in_qtr_and_qtd,
        available_renewals_open_past_due_qtd AS ren_available_renewals_open_past_due_qtd,
        available_renewals_lost_qtd AS ren_available_renewals_lost_qtd
    FROM {{ ref('tva_new_and_exp_by_qtr') }}
    LEFT JOIN {{ ref('tva_ren_by_qtr') }} ON tva_new_and_exp_by_qtr.qtr = tva_ren_by_qtr.qtr 
    JOIN {{ source('sales_and_cs_gsheets','forecast') }} ON tva_new_and_exp_by_qtr.qtr = forecast.qtr 
    LEFT JOIN ww_nn_amounts ON ww_nn_amounts.qtr = tva_new_and_exp_by_qtr.qtr
    LEFT JOIN ww_ren_amounts ON ww_ren_amounts.qtr = tva_new_and_exp_by_qtr.qtr
    LEFT JOIN ww_available_renewals ON ww_available_renewals.qtr = tva_new_and_exp_by_qtr.qtr
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41
)

SELECT * FROM scrub_ww