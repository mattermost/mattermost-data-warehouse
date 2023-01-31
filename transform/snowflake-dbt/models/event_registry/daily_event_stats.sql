{{
    config({
        "materialized": "table",
        "tags":"hourly",
        "schema": "event_registry",
        "cluster_by": ['event_date', 'source'],
    })
}}
-- depends_on: {{ ref('base_portal_prod') }}
-- depends_on: {{ ref('base_mattermostcom') }}
-- depends_on: {{ ref('base_incident_response_prod') }}
-- depends_on: {{ ref('base_mattermost_docs') }}
-- depends_on: {{ ref('base_hacktoberboard_prod') }}
-- depends_on: {{ ref('base_mm_mobile_prod') }}
-- depends_on: {{ ref('base_mm_plugin_prod') }}
-- depends_on: {{ ref('base_mm_telemetry_prod') }}

-- Schemas containing telemetry data.
{%
    set schemas =  [
        'portal_prod',
        'mattermostcom',
        'incident_response_prod',
        'mattermost_docs',
        'hacktoberboard_prod',
        'mm_mobile_prod',
        'mm_plugin_prod',
        'mm_telemetry_prod'
    ]
%}

WITH
{% for schema_ in schemas %}
    base_{{schema_}} AS (
    SELECT
        date_received_at AS event_date
        , event_table
        , event_name
        , '{{schema_}}' AS source
        , event_count
    FROM
        {{ ref('base_' + schema_) }}
    )
    {% if not loop.last -%}, {%- endif %}
{% endfor %}
{% for schema_ in schemas %}
-- A few conventions followed here:
-- - Timestamps/date names must end with _at
-- - Count columns must end with _count
-- - Columns that are useful for internal functionality have _ as a prefix
SELECT
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.surrogate_key(['event_date', 'event_table', 'source']) }} AS id
    , *
FROM
    base_{{schema_}}
    {% if not loop.last -%} UNION ALL {%- endif %}
{% endfor %}

