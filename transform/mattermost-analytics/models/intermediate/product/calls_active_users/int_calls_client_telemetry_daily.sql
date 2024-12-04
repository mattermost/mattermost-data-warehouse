{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "merge",
        "merge_update_columns": ['received_at_date'],
        "unique_key": ['daily_user_id'],
        "cluster_by": ['received_at_date'],
        "snowflake_warehouse": "transform_l"
    })
}}

with tmp as (
    select
        max(cast(received_at as date)) as received_at_date,
        cast(timestamp as date) as activity_date,
        server_id,
        user_id
    from
        {{ ref('stg_mm_calls_test_go__tracks') }}
    where
        -- Exclude items without user ids
        user_id is not null
        -- Exclude items without server ids
        and server_id is not null
        -- Exclude items with missing timestamps
        and timestamp is not null
        and received_at <= current_timestamp
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}
    group by activity_date, server_id, user_id
    qualify row_number() over (partition by activity_date, server_id, user_id order by received_at_date desc) = 1
)
select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , true as is_active
    -- Required for incremental loading
    , received_at_date
from
    tmp
where
    activity_date >= '{{ var('calls_telemetry_start_date')}}'
