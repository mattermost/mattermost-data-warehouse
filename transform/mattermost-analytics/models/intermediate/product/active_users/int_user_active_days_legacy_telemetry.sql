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
        cast(m2t.timestamp as date) as activity_date,
        server_id,
        user_id,
        client_type
    from
        {{ ref('stg_mattermost2__tracks') }} m2t 
    left join {{ ref('int_user_agent_parser') }} uap 
        on m2t.context_user_agent = uap.context_user_agent
    where
        -- Exclude items without user ids, such as server side telemetry etc
        user_id is not null
        -- Exclude items without server ids
        and server_id is not null
        -- Exclude items with missing timestamps
        and m2t.timestamp is not null
        and received_at <= current_timestamp
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}
    group by received_at_date, activity_date, server_id, user_id, client_type
    order by received_at_date
)
select
    -- Surrogate key required as it's both a good practice, as well as allows merge incremental strategy.
    {{ dbt_utils.generate_surrogate_key(['received_at_date', 'activity_date', 'server_id', 'user_id']) }} as daily_user_id
    , activity_date
    , server_id
    , user_id
    , case when lower(client_type:browser_family) = 'electron' then 'Desktop' 
    when lower(client_type:browser_family) != 'electron' then 'Webapp' end as client_type    
    , true as is_active
    -- Required for incremental loading
    , received_at_date
from
    tmp
where
    activity_date >= '{{ var('telemetry_start_date')}}'