-- depends_on: {{ ref('tracking_plan') }}

{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['event_id'],
        "snowflake_warehouse": "transform_l",
    })
}}

{# Load rules from tracking plan #}

{%- set feature_mappings = load_feature_mappings() -%}

select
    cast(received_at as date) as received_at_date
    , cast(timestamp as date) as activity_date
    , server_id
    , user_id
    , event_id
    , event_name
    , case
    {% for feature, rules in feature_mappings.items() %}
        {% for rule in rules %}
        when
            event_name = '{{ rule["EVENT_NAME"] }}'
            and category = '{{ rule["CATEGORY"] }}'
            and event_type = '{{ rule["EVENT_TYPE"] }}'
        {%- if rule["PROPERTY_NAME"] and rule["PROPERTY_VALUE"] is none -%}
            and {{rule["PROPERTY_NAME"] }} is not null
        {% endif %}
        {%- if rule["PROPERTY_NAME"] and rule["PROPERTY_VALUE"] is not none -%}
            and {{ rule["PROPERTY_NAME"] }} = '{{ rule["PROPERTY_VALUE"] }}'
        {% endif %}
            then '{{ feature }}'
        {% endfor %}
    {% endfor %}
        else '{{ var("const_unknown_features") }}'
    end as feature_name
from
    {{ ref('stg_mm_telemetry_prod__event_deduped') }}
where
    -- Exclude items without user info
    user_id is not null
    -- Exclude items without server ids
    and server_id is not null
    -- Exclude items with missing timestamps
    and timestamp is not null
    -- Exclude items from the future
    and received_at <= current_timestamp
{% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    and received_at >= (select max(received_at_date) from {{ this }})
{% endif %}