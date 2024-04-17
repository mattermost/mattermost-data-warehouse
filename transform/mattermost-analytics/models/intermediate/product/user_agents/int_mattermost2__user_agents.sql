{{
    config({
        "materialized": "incremental",
        "unique_key": ['user_agent_id'],
        "snowflake_warehouse": "transform_l", 
        "incremental_strategy": "merge",
        "merge_update_columns": ['timestamp'],
    })
}}

select context_user_agent as context_user_agent,
        max(timestamp) as timestamp,
        md5(context_user_agent) as user_agent_id
    from {{ ref('stg_mattermost2__tracks') }} mtp
    where
    -- Exclude rows without context_user_agent
        context_user_agent is not null        
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and timestamp >= (select max(timestamp) from {{ this }})
{% endif %}
    group by context_user_agent
