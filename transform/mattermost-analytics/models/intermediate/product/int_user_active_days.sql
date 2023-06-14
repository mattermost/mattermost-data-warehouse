{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "unique_key": ['daily_user_id'],
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

with tmp as (
    select
        cast(received_at as date) as received_at_date,
        cast(timestamp as date) as activity_date,
        user_id as server_id,
        user_actual_id as user_id
    from
        {{ ref('stg_mm_telemetry_prod__tracks') }}
    where
       received_at <= current_timestamp
{% if is_incremental() %}
       -- this filter will only be applied on an incremental run
      and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}
    group by received_at_date, activity_date, user_id, user_actual_id
    order by received_at_date, user_id
)
select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'source', 'user_id', 'server_id']) }} AS daily_user_id
    , received_at_date
    , activity_date
    , server_id
    , user_id
    , 1 as is_active
from
    tmp