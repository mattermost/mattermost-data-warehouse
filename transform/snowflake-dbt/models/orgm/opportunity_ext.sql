{{config({
    "materialized": "table",
    "schema": "orgm"
  })
}}


WITH opportunity_ext AS (
    SELECT
        opportunityid,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'New' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_new_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Expansion' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_expansion_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Ren' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_renewal_amount,
        SUM(CASE WHEN opportunitylineitem.product_line_type__c = 'Multi' THEN opportunitylineitem.totalprice ELSE 0 END) AS sum_multi_amount
    FROM {{ source('orgm', 'opportunitylineitem') }}
    GROUP BY 1
)

SELECT * FROM opportunity_ext