{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , {{ dbt_utils.pivot(
          'feature_name',
          dbt_utils.get_column_values(ref('int_server_feature_attribution'), 'feature_name'),
          agg='sum',
          prefix='count_'
      ) }}
    , count(event_id) as count_total
from
    {{ ref('int_server_feature_attribution') }}
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
