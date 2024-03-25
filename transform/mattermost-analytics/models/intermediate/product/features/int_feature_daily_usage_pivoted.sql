--
-- Creates a table where each row holds which known features were used by each server/user.
--
with feature_aliases as (
    -- Get feature alias for each feature. This is required to get each feature as a column.
    select
        f.event_name
        , f.category
        , f.event_type
        , a.alias as feature_name
    from
        {{ ref('event_to_feature_mapping') }} f
        join {{ ref('feature_aliases') }} a on f.feature_name = a.feature_name

), feature_daily_usage as (
    -- Create a matrix of daily feature usage per user.
    select
        u.activity_date
        , u.server_id
        , u.user_id
        -- Mark known features and use a bucket for the rest
        , coalesce(f.feature_name, 'unknown')
        , u.event_count
    from
        {{ ref('int_feature_daily_usage_per_user') }} u
        join feature_aliases f on u.event_name = f.event_name and u.category = f.category and f.event_type = u.event_type
)
-- Row per date, server, user. Contains one column per known feature.
select
    activity_date
    , server_id
    , user_id
    , {{
        dbt_utils.pivot(
            'feature_name',
            dbt_utils.get_column_values(ref('feature_aliases'), 'alias') + ['unknown'],
            agg='sum',
            then_value='event_count',
            quote_identifiers=False
        )
    }}
from
    feature_daily_usage
group by
    activity_date, server_id, user_id