{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}


WITH account_ext AS (
    SELECT
        account.sfid as account_sfid,
        COUNT(DISTINCT CASE WHEN opportunity.iswon THEN opportunity.sfid ELSE NULL END) AS count_won_oppt,
        COUNT(DISTINCT CASE WHEN NOT opportunity.isclosed THEN opportunity.sfid ELSE NULL END) AS count_open_oppt,
        COUNT(DISTINCT CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity.sfid ELSE NULL END) AS count_lost_oppt,
        SUM(opportunity_ext.sum_new_amount) AS sum_new_amount,
        SUM(opportunity_ext.sum_expansion_amount) AS sum_expansion_amount,
        SUM(opportunity_ext.sum_renewal_amount) AS sum_renewal_amount,
        SUM(opportunity_ext.sum_multi_amount) AS sum_multi_amount
    FROM {{ source('orgm', 'account') }}
    LEFT JOIN {{ source('orgm', 'opportunity') }} ON account.sfid = opportunity.accountid
    LEFT JOIN {{ ref('opportunity_ext') }} ON account.sfid = opportunity_ext.accountid
    GROUP BY 1
)

SELECT * FROM account_ext