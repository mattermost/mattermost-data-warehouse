{{config({
    "materialized": 'table',
    "schema": "cs"
  })
}}

WITH account_renewal_rate_by_qtr AS (
    SELECT
        available_renewals.account_sfid,
        available_renewals.license_end_qtr,
        available_renewals.available_renewals,
        sum(renewal_amount__c) AS renewal_amount,
        sum(CASE WHEN opportunity.iswon OR NOT opportunity.isclosed THEN opportunity.probability/100.00 * opportunitylineitem.renewal_amount__c ELSE 0 END) AS forecasted_renewal_amount,
        least(available_renewals.available_renewals, sum(CASE WHEN opportunity.iswon THEN opportunitylineitem.renewal_amount__c ELSE 0 END)) AS gross_renewal_amount,
        least(available_renewals.available_renewals, sum(CASE WHEN NOT opportunity.isclosed THEN opportunitylineitem.renewal_amount__c ELSE 0 END)) AS gross_open_renewal_total_amount,
        least(available_renewals.available_renewals, sum(CASE WHEN NOT opportunity.isclosed THEN opportunity.probability/100.00 * opportunitylineitem.renewal_amount__c ELSE 0 END)) AS gross_forecasted_renewal_total_amount,
        round(100*coalesce(least(available_renewals.available_renewals, sum(CASE WHEN opportunity.iswon THEN opportunitylineitem.renewal_amount__c ELSE 0 END))::float/available_renewals.available_renewals,0),2) AS account_renewal_rate_by_qtr,
        round(100*coalesce(least(available_renewals.available_renewals, sum(CASE WHEN opportunity.iswon OR NOT opportunity.isclosed THEN opportunity.probability/100.00 * opportunitylineitem.totalprice ELSE 0 END))::float/available_renewals.available_renewals,0),2) AS account_forecast_renewal_rate_by_qtr,
        round(100*coalesce(least(available_renewals.available_renewals, sum(CASE WHEN opportunity.iswon OR NOT opportunity.isclosed THEN opportunitylineitem.renewal_amount__c ELSE 0 END))::float/available_renewals.available_renewals,0),2) AS account_max_renewal_rate_by_qtr,
        least(available_renewals.available_renewals, sum(CASE WHEN opportunity.iswon AND (util.fiscal_year(opportunity.closedate::date)||'-'||util.fiscal_quarter(opportunity.closedate::date)) = available_renewals.license_end_qtr THEN opportunitylineitem.renewal_amount__c ELSE 0 END)) AS gross_renewal_amount_in_qtr,
        round(100*coalesce(least(available_renewals.available_renewals, sum(CASE WHEN opportunity.iswon AND (util.fiscal_year(opportunity.closedate::date)||'-'||util.fiscal_quarter(opportunity.closedate::date)) = available_renewals.license_end_qtr THEN opportunitylineitem.renewal_amount__c ELSE 0 END))::float/available_renewals.available_renewals,0),2) AS account_renewal_rate_in_qtr
    FROM {{ source('cs','account_available_renewals_by_qtr') }} AS available_renewals
    LEFT JOIN {{ source('orgm','account') }} ON account.sfid = available_renewals.account_sfid
    LEFT JOIN {{ source('orgm','opportunity') }} ON opportunity.accountid = account.sfid
        AND available_renewals.license_end_qtr = (util.fiscal_year(opportunity.license_start_date__c::date - interval '1 day') ||'-'|| util.fiscal_quarter(opportunity.license_start_date__c::date - interval '1 day'))
    LEFT JOIN {{ source('orgm','opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    WHERE available_renewals.available_renewals > 0
    GROUP BY 1, 2, 3
)

SELECT * FROM account_renewal_rate_by_qtr
