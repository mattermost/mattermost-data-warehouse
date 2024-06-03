-- Forcing dependency due to load_feature_mappings macro
-- depends_on: {{ ref('tracking_plan') }}

{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['daily_user_id'],
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}


{# Load rules from tracking plan #}
{%- set feature_mappings = load_feature_mappings() -%}

select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['received_at_date', 'activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , received_at_date
{% for feature in feature_mappings.keys() %}
    , count_if({{feature}}) as count_{{feature}}
{% endfor %}
    , count(event_id) as count_total_events
from
    {{ ref('int_feature_attribution') }}
{% if is_incremental() %}
where
    -- this filter will only be applied on an incremental run
    received_at_date >= (select max(received_at_date) from {{ this }})
{% endif %}
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
    , received_at_date