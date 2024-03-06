{{
    config({
        "materialized": "table",
    })
}}

--
-- Creates a table where each row holds which paid features were used by each server/user.
--
with feature_aliases as (
    -- Get feature alias for each feature. This is required to get each feature as a column.
    select
        f.event_name
        , f.category
        , f.event_type
        , a.alias as feature_name
    from
        {{ ref('paid_features') }} f
        join {{ ref('paid_feature_aliases') }} a on f.feature_name = a.feature_name

), paid_feature_daily_usage as (
    -- Keep only events for paid features.
    select
        u.activity_date
        , u.server_id
        , u.user_id
        , f.feature_name
        , u.event_count
    from
        {{ ref('int_feature_daily_usage_per_user') }} u
        join feature_aliases f on u.event_name = f.event_name and u.category = f.category and f.event_type = u.event_type
)
-- Row per date, server, user. Contains one column per paid feature.
select
    activity_date
    , server_id
    , user_id
    , {{
        dbt_utils.pivot(
            'feature_name',
            dbt_utils.get_column_values(ref('paid_feature_aliases'), 'alias'),
            agg='sum',
            then_value='event_count',
            prefix='count_feature_',
            suffix='_events',
            quote_identifiers=False
        )
    }}
from
    paid_feature_daily_usage
group by
    activity_date, server_id, user_id