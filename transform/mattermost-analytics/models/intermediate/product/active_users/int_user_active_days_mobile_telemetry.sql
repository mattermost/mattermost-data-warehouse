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
        cast(received_at as date) as received_at_date,
        cast(timestamp as date) as activity_date,
        server_id,
        user_id
    from
        {{ ref('stg_mm_mobile_prod__tracks') }}
    where
        -- Exclude items without user ids, such as server side telemetry etc
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
    group by received_at_date, activity_date, server_id, user_id
    order by received_at_date
)
select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , true as is_active
    -- Required for incremental loading
    -- Use max to ensure that the most recent received_at_date is used
    , max(received_at_date) as received_at_date
from
    tmp
where
    activity_date >= '{{ var('telemetry_start_date')}}'
group by activity_date, server_id, user_id