{{
    config({
        "cluster_by": ['event_date'],
    })
}}

select
    {{dbt_utils.star(ref('stg_copilot__tracks'))}}
from
    {{ ref('stg_copilot__tracks') }}