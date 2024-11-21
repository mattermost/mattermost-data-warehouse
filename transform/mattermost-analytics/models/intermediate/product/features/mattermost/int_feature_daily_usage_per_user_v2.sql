{{
    config({
        "materialized": "table",
        "snowflake_warehouse": "transform_l"
    })
}}

with daily_usage as (
    {{ dbt_utils.union_relations(
        relations=[ref('int_client_feature_attribution'), ref('int_server_feature_attribution']
    ) }}
)
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
    , count(event_id) as count_total
from
    daily_usage
group by
    daily_user_id
    , activity_date
    , server_id
    , user_id
