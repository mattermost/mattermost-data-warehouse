
{{config({
    "materialized": "incremental",
    "unique_key": 'id',
    "schema": "orgm"
  })
}}

WITH opportunity_totals AS (
    SELECT 
        opportunityid,
        SUM(new_amount__c) + SUM(expansion_amount__c) + SUM(coterm_expansion_amount__c) AS net_new_amount,
        SUM(renewal_amount__c) AS renewal_amount
    FROM {{ source('orgm', 'opportunitylineitem') }}
    GROUP BY 1
), opportunity_snapshot AS (
    SELECT
        {{ dbt_utils.surrogate_key('current_date', 'sfid') }} AS id,
        current_date AS snapshot_date,
        sfid,
        name, 
        ownerid,
        csm_owner__c,
        type, 
        closedate,
        iswon,
        isclosed,
        probability,
        stagename,
        status_wlo__c,
        forecastcategoryname, 
        expectedrevenue, 
        amount,
        net_new_amount,
        renewal_amount,
        territory_segment__c
        FROM {{ source('orgm', 'opportunity') }}
        LEFT JOIN opportunity_totals ON opportunity.sfid = opportunity_totals.opportunityid
        {% if is_incremental() %}
        WHERE current_date >= (SELECT MAX(snapshot_date) FROM {{this}})
        {% endif %}
)
SELECT * FROM opportunity_snapshot