{{
    config({
        "materialized": "table",
    })
}}

select
    *
from
    {{ ref('int_daily_server_nps_score') }}