
{{config({
    "materialized": "incremental",
    "unique_key": 'id',
    "schema": "orgm"
  })
}}

WITH opportunitylineitem_snapshot AS (
    SELECT
        {{ dbt_utils.surrogate_key('current_date', 'sfid') }} AS id,
        current_date AS date,
        sfid,
        name,
        opportunityid,
        product2id,
        product_type__c,
        description,
        listprice,
        unitprice,
        discount,
        quantity,
        totalprice,
        start_date__c,
        end_date__c
        arr_contributed__c,
        revenue_type__c,
        product_line_type__c,
        new_amount__c,
        renewal_amount__c,
        expansion_amount__c,
        coterm_expansion_amount__c,
        leftover_expansion_amount__c,
        multi_amount__c
        FROM {{ source('orgm', 'opportunitylineitem') }}
        {% if is_incremental() %}
        WHERE current_date > (SELECT MAX(date) FROM {{this}})
        {% endif %}
)
SELECT * FROM opportunitylineitem_snapshot