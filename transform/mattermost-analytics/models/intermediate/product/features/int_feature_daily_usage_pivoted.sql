--
-- Creates a table where each row holds which known features were used by each server/user.
--
with servers_with_known_features as (
    -- Keep only servers with at least one known feature. This reduces the execution time of the current query.
    -- This filter can be removed in the future if there's a need to expand the breakdown even for servers without any
    -- known features.
    select
        distinct server_id
    from
        {{ ref('int_mattermost_daily_usage_per_user') }}
    where
        feature_alias <> 'unknown'

    union

    select
        distinct server_id
    from
        {{ ref('int_playbooks_daily_usage_per_user') }}
    where
        feature_alias <> 'unknown'
), feature_daily_usage as (
    -- Create a matrix of daily feature usage per user.
    select
        u.activity_date
        , u.server_id
        , u.user_id
        , u.feature_alias
        , u.event_count
    from
        {{ ref('int_mattermost_daily_usage_per_user') }} u
    where
        userver_id
        join servers_with_known_features s on u.server_id = s.server_id


    union all

    select
        u.activity_date
        , u.server_id
        , u.user_id
        , u.feature_alias
        , u.event_count
    from
        {{ ref('int_playbooks_daily_usage_per_user') }} u
        join servers_with_known_features s on u.server_id = s.server_id

)
-- Row per date, server, user. Contains one column per known feature.
select
    activity_date
    , server_id
    , user_id
    , {{ dbt_utils.pivot(
            'feature_alias',
            dbt_utils.get_column_values(ref('feature_aliases'), 'alias') + ['unknown'],
            agg='sum',
            then_value='event_count',
            quote_identifiers=False
        ) }}
from
    feature_daily_usage
group by
    activity_date, server_id, user_id