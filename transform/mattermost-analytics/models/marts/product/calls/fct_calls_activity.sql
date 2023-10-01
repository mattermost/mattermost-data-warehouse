{{
    config({
        "materialized": "table",
        "cluster_by": ['activity_date', 'server_id'],
    })
}}

select
    {{dbt_utils.star(ref('int_calls_active_days_spined'))}}
from
    {{ ref('int_calls_active_days_spined') }}