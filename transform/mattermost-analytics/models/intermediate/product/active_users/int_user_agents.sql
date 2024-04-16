{{
    config({
        "materialized": "incremental",
        "unique_key": ['user_agent_id'],
        "snowflake_warehouse": "transform_l", 
        "incremental_strategy": "merge",
        "merge_update_columns": ['timestamp'],
    })
}}


with tmp as (
    select coalesce(mtp.context_user_agent, mm2.context_user_agent) as context_user_agent,
        max(coalesce(mtp.timestamp, mm2.timestamp)) as timestamp,
        md5(coalesce(mtp.context_user_agent, mm2.context_user_agent)) as user_agent_id
    from {{ ref('stg_mm_telemetry_prod__tracks') }} mtp
        full outer join {{ ref('stg_mattermost2__tracks') }} mm2 
        on mtp.context_user_agent = mm2.context_user_agent
    where
    -- Exclude rows without context_user_agent
        mtp.context_user_agent is not null 
        or 
        mm2.context_user_agent is not null        
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and coalesce(mtp.timestamp, mm2.timestamp) >= (select max(max_timestamp) from {{ this }})
{% endif %}
    group by coalesce(mtp.context_user_agent, mm2.context_user_agent)
),
parsed as (
    select tmp.*, 
    {% if is_incremental() %}
        case 
            when previously_parsed.user_agent_id is not null 
            then previously_parsed.user_agent
            else coalesce(parse_user_agent(context_user_agent), null)
        end as user_agent
    from tmp tmp 
        left join {{ this }} previously_parsed on tmp.user_agent_id = previously_parsed.user_agent_id
    {% else %}
    iff(context_user_agent is not null, parse_user_agent(context_user_agent), null) as user_agent
    from tmp
    {% endif %}
)
select *
from parsed
