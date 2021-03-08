{{config({
    "materialized": 'table',
    "schema": "cs"
  })
}}

WITH renewal_rate_by_renewal_opportunity AS (
    SELECT
        account.sfid  AS accountid,
	    opportunity.sfid  AS opportunityid,
        available_renewals_by_opportunity.renewal_opportunity AS google_sheet_renewal_opportunity,
	    available_renewals_by_opportunity.renewal_qtr,
        available_renewals_by_opportunity.renewal_license_start::date - interval '1 day' AS renewal_date,
        available_renewals_by_opportunity.renewal_license_start::date as renewal_license_start,
	    available_renewals_by_opportunity.available_renewal,
	    SUM(CASE WHEN status_wlo__c = 'Won' THEN renewal_amount__c ELSE 0 END) AS won_renewal_net_total,
        SUM(CASE WHEN status_wlo__c = 'Open' THEN renewal_amount__c ELSE 0 END) AS open_renewal_net_total,
        SUM(CASE WHEN status_wlo__c = 'Lost' THEN renewal_amount__c ELSE 0 END) AS lost_renewal_net_total,
        LEAST(SUM(CASE WHEN status_wlo__c = 'Won' THEN renewal_amount__c ELSE 0 END),available_renewals_by_opportunity.available_renewal) AS won_renewal_gross_total,
        LEAST(SUM(CASE WHEN status_wlo__c = 'Open' THEN renewal_amount__c ELSE 0 END),available_renewals_by_opportunity.available_renewal) AS open_renewal_gross_total,
        CASE WHEN status_wlo__c = 'Lost' THEN available_renewals_by_opportunity.available_renewal ELSE 0 END AS lost_renewal_gross_total,
        ROUND(LEAST(SUM(CASE WHEN status_wlo__c = 'Won' THEN renewal_amount__c ELSE 0 END),available_renewals_by_opportunity.available_renewal)*1.00/available_renewals_by_opportunity.available_renewal,4) as ren_rate,
        ROUND(LEAST(SUM(CASE WHEN status_wlo__c != 'Lost' THEN renewal_amount__c ELSE 0 END),available_renewals_by_opportunity.available_renewal)*1.00/available_renewals_by_opportunity.available_renewal,4) as max_ren_rate
    FROM {{ ref('available_renewal_by_opportunity') }} AS available_renewals_by_opportunity
    LEFT JOIN {{ ref('account') }} ON account.sfid = available_renewals_by_opportunity.account_id
    LEFT JOIN {{ ref('opportunity') }} ON opportunity.sfid = available_renewals_by_opportunity.renewal_opportunity
    LEFT JOIN {{ ref('opportunitylineitem') }} ON opportunity.sfid = opportunitylineitem.opportunityid
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 13
)

SELECT * FROM renewal_rate_by_renewal_opportunity
