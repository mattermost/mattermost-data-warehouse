
{% macro rudder_tracks_summary(schema) -%}
WITH tmp AS (
    SELECT
        -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
        CAST(received_at AS date) AS date_received_at
        , event AS event_table
        , event_text AS event_name
        , COUNT(*) AS event_count
    FROM
        {{ source(schema, 'tracks') }}
    WHERE
       received_at <= CURRENT_TIMESTAMP
{% if is_incremental() %}
       -- this filter will only be applied on an incremental run
      AND received_at >= (SELECT MAX(date_received_at) FROM {{ this }})
{% endif %}
    GROUP BY 1, 2, 3
    ORDER BY 1, 2
)
SELECT
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.surrogate_key(['date_received_at', 'event_table']) }} AS id
    , date_received_at
    , event_table
    , event_name
    , event_count
FROM
    tmp
{%- endmacro%}