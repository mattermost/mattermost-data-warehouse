-- depends_on: {{ ref('tracking_plan') }}

{{
    config({
        "materialized": "incremental",
        "incremental_strategy": "delete+insert",
        "unique_key": ['event_id'],
        "snowflake_warehouse": "transform_l"
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
    , category
    , event_type
    {% for feature, rules in feature_mappings.items() %}
    , case
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
            then true
    {% endfor %}
        else false
    end as {{ feature }}
{% endfor %}
    , not ({{ ' or '.join(feature_mappings.keys()) }}) as unknown_feature
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
    -- Incremental with reconcile for late arriving events up to two days.
    -- Event received in the past two days
    received_at >= (select dateadd(day, -2, max(received_at_date)) from {{ this }})
    -- Event has not been merged to the current table
    and event_id not in (select event_id from {{ this }} where received_at >= (select dateadd(day, -2, max(received_at_date)) from {{ this }}))
{% endif %}