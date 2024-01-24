{{
    config({
        "materialized": "table",
    })
}}

select
    {{dbt_utils.star(ref('int_daily_server_nps_score'))}}
from
    {{ ref('int_daily_server_nps_score') }}