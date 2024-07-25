{{
    config({
        "cluster_by": ['event_date'],
    })
}}

select
    {{dbt_utils.star(ref('stg_copilot__tracks'))}}
   , timestamp::date as event_date
from
    {{ ref('stg_copilot__tracks') }}