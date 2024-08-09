{{
    config({
        "materialized": "incremental",
        "cluster_by": ['received_at_date'],
    })
}}

SELECT {{ dbt_utils.star(ref('stg_mm_telemetry_rc__performance_events')) }} FROM 
{{ ref('stg_mm_telemetry_rc__performance_events') }} 
WHERE
    id not in (
        'd\';waitfor/**/delay\'0:0:0\'/**/--/**/',
        's\');waitfor/**/delay\'0:0:0\'/**/--/**/',
        'v\';waitfor/**/delay\'0:0:2\'/**/--/**/',
        'o\');waitfor/**/delay\'0:0:0\'/**/--/**/',
        'z\';waitfor/**/delay\'0:0:0\'/**/--/**/'
    )
{% if is_incremental() %}
   and received_at > (SELECT MAX(received_at) FROM {{ this }})
{% endif %}