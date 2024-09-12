-- Forcing dependency due to load_feature_mappings macro
-- depends_on: {{ ref('tracking_plan') }}

{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}


{# Load rules from tracking plan #}
{%- set feature_mappings = load_feature_mappings() -%}

select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
{% for feature in feature_mappings.keys() %}
    -- , count_if({{feature}}) as count_{{feature}}
    -- Dedupe version
    , ARRAY_SIZE(ARRAY_UNIQUE_AGG(case when {{feature}} then event_id end)) as  count_{{feature}}
    -- , count(distinct case when {{feature}} then event_id end) as count_{{feature}}
{% endfor %}
    , (
    {% for feature in feature_mappings.keys() %}
         count_{{feature}}  {%- if not loop.last %} + {% endif -%}
    {% endfor %}
    ) as count_known_features
    , count_if(unknown_feature) as count_unknown_features
--     , count(event_id) as count_total
    -- Dedupe version
    , ARRAY_SIZE(ARRAY_UNIQUE_AGG(case when unknown_feature then event_id end)) as count_unknown_features
    -- , count(distinct case when unknown_feature then event_id end) as count_unknown_features
    -- , count(distinct event_id) as count_total
from
    {{ ref('int_feature_attribution') }}
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
