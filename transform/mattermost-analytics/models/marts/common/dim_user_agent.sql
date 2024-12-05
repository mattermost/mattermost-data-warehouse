{{
    config({
        "materialized": "incremental",
        "unique_key": 'user_agent_id',
        "incremental_strategy": "append",
        "snowflake_warehouse": "transform_l"
    })
}}
select
    user_agent_id,
    user_agent:browser_family::varchar as browser_family,
    user_agent:browser_version_major::int as browser_version_major,
    user_agent:browser_version_minor::int as browser_version_minor,
    user_agent:browser_version_patch::varchar as browser_version_patch,
    user_agent:device_brand::varchar as device_brand,
    user_agent:device_family::varchar as device_family,
    user_agent:device_model::varchar as device_model,
    user_agent:os_family::varchar as os_family,
    user_agent:os_version_major::varchar as os_version_major,
    user_agent:os_version_minor::varchar as os_version_minor,
    user_agent:os_version_patch::varchar as os_version_patch,
    user_agent:os_version_patch_minor::varchar as os_version_patch_minor,
    context_user_agent as raw_user_agent
from
    {{ ref('int_user_agents') }}
{% if is_incremental() %}
where
    user_agent_id not in (select user_agent_id from {{ this }})
{% endif %}