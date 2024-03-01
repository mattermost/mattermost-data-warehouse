with paid_feature_daily_usage as (
    select
    {{ dbt_utils.star(from=ref('int_feature_daily_usage_per_user'), relation_alias='u') }},
    f.feature_name
    from
        {{ ref('int_feature_daily_usage_per_user') }} u
        join {{ ref('paid_features') }} f on u.event_name = f.event_name and u.category = f.category and f.event_type = u.event_type
)
select * from paid_feature_daily_usage