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
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_new_amount ELSE 0 END) AS sum_new_amount_won,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_expansion_amount ELSE 0 END) AS sum_expansion_amount_won,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_renewal_amount ELSE 0 END) AS sum_renewal_amount_won,
        SUM(CASE WHEN opportunity.iswon THEN opportunity_ext.sum_multi_amount ELSE 0 END) AS sum_multi_amount_won,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_new_amount ELSE 0 END) AS sum_new_amount_open,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_expansion_amount ELSE 0 END) AS sum_expansion_amount_open,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_renewal_amount ELSE 0 END) AS sum_renewal_amount_open,
        SUM(CASE WHEN NOT opportunity.isclosed THEN opportunity_ext.sum_multi_amount ELSE 0 END) AS sum_multi_amount_open,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_new_amount ELSE 0 END) AS sum_new_amount_lost,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_expansion_amount ELSE 0 END) AS sum_expansion_amount_lost,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_renewal_amount ELSE 0 END) AS sum_renewal_amount_lost,
        SUM(CASE WHEN opportunity.isclosed AND NOT opportunity.iswon THEN opportunity_ext.sum_multi_amount ELSE 0 END) AS sum_multi_amount_lost
    FROM {{ source('orgm', 'account') }}
    LEFT JOIN {{ source('orgm', 'opportunity') }} ON account.sfid = opportunity.accountid
    LEFT JOIN {{ ref('opportunity_ext') }} ON account.sfid = opportunity_ext.accountid
    GROUP BY 1
)

SELECT * FROM account_ext