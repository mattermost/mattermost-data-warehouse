{{
    config({
        "materialized": "table"
    })
}}


{%
    set column_names = dbt_utils.get_filtered_columns_in_relation(
        from=ref('int_feature_daily_usage_per_user'),
        except=["daily_user_id", "activity_date", "server_id", "user_id", "received_at_date", "count_unknown_feature"]
    )
%}


select
    {{ dbt_utils.star(from=ref('int_feature_daily_usage_per_user'), except=["received_at_date"]) }}
from
    {{ ref('int_feature_daily_usage_per_user') }}
where
-- Keep only servers with at least one known value
    false
{% for column_name in column_names %}
    or {{ column_name }} > 0
{% endfor %}

