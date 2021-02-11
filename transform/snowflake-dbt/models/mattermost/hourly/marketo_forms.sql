{{config({
    "materialized": "incremental",
    "schema": "mattermost",
    "unique_key":"id",
    "tags":"hourly"
  })
}}

{% if is_incremental() %}
WITH max_time AS (
    SELECT MAX(timestamp) - interval '6 hours' AS max_timestamp
    FROM {{this}}
),
{% else %}
WITH 

{% endif %}

marketo_forms AS (
    SELECT mkto.* 
    FROM {{ source('mattermostcom', 'marketo_form_submit')}} mkto
    WHERE mkto.timestamp::date <= CURRENT_DATE
    {% if is_incremental() %}
    AND mkto.timestamp >= (SELECT max_timestamp from max_time)
    {% endif %}
)

SELECT *
FROM marketo_forms