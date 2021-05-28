{% if execute %}
  {% if flags.FULL_REFRESH %}
      {{ exceptions.raise_compiler_error("Full refresh is not allowed for this model. Exclude it from the run via the argument \"--exclude acount_snapshot\".") }}
  {% endif %}
{% endif %}

{{config({
    "materialized": "incremental",
    "unique_key": 'id',
    "schema": "orgm"
  })
}}

WITH account_snapshot AS (
    SELECT
        {{ dbt_utils.surrogate_key(['current_date', 'sfid']) }} AS id,
        current_date AS snapshot_date,
        sfid,
        name, 
        ownerid,
        csm_lookup__c,
        parentid,
        type,
        website,
        cbit__clearbitdomain__c,
        company_type__c,
        arr_current__c,
        territory_region__c,
        territory_comm_rep__c,
        territory_area__c,
        territory_ent_rep__c,
        territory_geo__c,
        territory_segment__c,
        territory_last_updated__c
        FROM {{ ref('account') }}
        {% if is_incremental() %}
        WHERE current_date >= (SELECT MAX(snapshot_date) FROM {{this}})
        {% endif %}
)
SELECT * FROM account_snapshot