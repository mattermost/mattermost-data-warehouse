{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude opportunity_snapshot\".") }}
  {% endif %}
{% endif %}

{{config({
    "materialized": "incremental",
    "unique_key": 'id',
    "schema": "orgm"
  })
}}

WITH opportunity_totals AS (
    SELECT 
        opportunityid,
        SUM(new_amount__c) as new,
        SUM(expansion_amount__c) expansion, 
        SUM(coterm_expansion_amount__c) as coterm,
        SUM(leftover_expansion_amount__c) as loe,
        SUM(renewal_amount__c) AS renewal,
        SUM(multi_amount__c) AS multi,
        SUM(renewal_multi_amount__c) AS renewal_multi,
        SUM(monthly_billing_amount__c) AS monthly_billing
    FROM {{ ref('opportunitylineitem') }}
    GROUP BY 1
), opportunity_snapshot AS (
    SELECT
        {{ dbt_utils.surrogate_key(['current_date', 'sfid']) }} AS id,
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
        new + expansion + coterm + loe as net_new_amount,
        renewal as renewal_amount,
        territory_segment__c,
        new,
        expansion,
        coterm,
        loe,
        renewal,
        multi,
        renewal_multi,
        monthly_billing,
        cpq_renewal_arr__c, 
        exit_year_arr__c, 
        sales_forecast_category__c,  
        total_net_new_arr_with_override__c
        FROM {{ ref('opportunity') }}
        LEFT JOIN opportunity_totals ON opportunity.sfid = opportunity_totals.opportunityid
        {% if is_incremental() %}
        WHERE current_date >= (SELECT MAX(snapshot_date) FROM {{this}})
        {% endif %}
)
SELECT * FROM opportunity_snapshot