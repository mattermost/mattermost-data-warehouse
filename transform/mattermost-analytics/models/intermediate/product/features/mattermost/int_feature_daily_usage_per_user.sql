{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['daily_user_id'],
        "snowflake_warehouse": "transform_l"
    })
}}

{%-
    set values = dbt_utils.get_column_values(ref('int_mattermost_feature_attribution'), 'feature_name')
-%}
{%-
    set known_features = values | reject("==", var('const_unknown_features'))
-%}


{% if is_incremental() %}
with time_thresholds as (
    select
        source,
        max(received_at_date) as received_at_date
    from
        {{ ref('int_mattermost_feature_attribution') }}
    group by
        source
)
{% endif %}
select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , {{ dbt_utils.pivot(
          'feature_name',
          values,
          agg='sum',
          prefix='count_',
          quote_identifiers=False
      ) }}
    , {% for val in known_features %}
         {% if not loop.first %} + {% endif -%} count_{{val}}
    {%- endfor %} as count_known_features
    , count(event_id) as count_total
from
    {{ ref('int_mattermost_feature_attribution') }}
{% if is_incremental() %}
where
    -- Recalculate the past 2 days from the earliest source before last received_at_date in order to include
    -- any late-arriving events to calculations. Another way to see it is that it recalculate already calculated data
    -- for a rolling window in order to reconcile for late-arriving events.
    received_at_date >= (select dateadd(day, -2, min(received_at_date)) from time_thresholds)
{% endif %}
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
