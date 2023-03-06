{% macro get_base_event_columns() -%}
    {{ return(['ID', 'RECEIVED_AT', 'EVENT', 'EVENT_TEXT', 'CATEGORY', 'TYPE', 'USER_ID', 'USER_ACTUAL_ID', 'TIMESTAMP']) }}
{%- endmacro%}