
{% macro rudder_daily_event_count(relation, by_columns=['event_table', 'event_text'], source_name=None) -%}

WITH tmp AS (
    SELECT
        -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
        CAST(received_at AS date) AS received_at_date
        {% for column_name in by_columns %}
        , {{ column_name }}
        {% endfor %}
        , COUNT(distinct event_id) AS event_count
        {% if source_name %}
        , '{{ source_name }}' AS source
        {% else %}
        , '{{ relation.identifier }}' AS source
        {% endif %}
    FROM
        {{ relation }}
    WHERE
       received_at <= CURRENT_TIMESTAMP
{% if is_incremental() %}
       -- this filter will only be applied on an incremental run
      AND received_at >= (SELECT MAX(received_at_date) FROM {{ this }})
{% endif %}
    GROUP BY received_at_date, {{ ', '.join(by_columns) }}
    ORDER BY received_at_date, {{ by_columns[0] }}
)
SELECT
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['received_at_date', 'source'] + by_columns ) }} AS daily_event_id
    , {{ dbt_utils.generate_surrogate_key(['source'] + by_columns ) }} AS event_id
    , received_at_date
    , source
    {% for column_name in by_columns %}
    , {{ column_name }}
    {% endfor %}
    , event_count
FROM
    tmp
{%- endmacro%}